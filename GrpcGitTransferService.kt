package com.tencent.bk.devops.code.manage.migration.service.impl

import com.tencent.bk.devops.code.common.web.util.SecretUtil
import com.tencent.bk.devops.code.manage.migration.pojo.bo.GitTransferResult
import com.tencent.bk.devops.code.manage.migration.service.GitTransferService
import com.tencent.bk.devops.code.manage.pojo.bo.Repo
import grpc.git.store.RepositoryServiceGrpc
import grpc.git.store.common.repoProto
import grpc.git.store.common.userProto
import grpc.git.store.pullRequest
import grpc.git.store.setMirrorRequest
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * 通过 git-store gRPC Pull 实现 Git 数据迁移。
 *
 * 参照 [RepoPullMirrorEventConsumerImpl] 中的 gRPC 调用模式：
 * - 构造 PullRequest，传入 repository、user、encryptedCredentials
 * - encryptedCredentials 使用 SM4 加密的 "username@password" 格式
 */
@Service
class GrpcGitTransferService(
    private val repositoryStub: RepositoryServiceGrpc.RepositoryServiceBlockingStub
) : GitTransferService {

    override fun transfer(
        repo: Repo,
        operatorName: String,
        operatorTenantId: String,
        token: String,
        sourceUsername: String,
        sourceCloneUrl: String,
    ): GitTransferResult {
        logger.info(
            "Starting gRPC Pull for repo[${repo.id}], namespacePath=${repo.identify()}, source=$sourceCloneUrl"
        )

        val repoProto = repoProto {
            this.tenantId = repo.tenantId
            this.groupId = repo.groupId
            this.bkcRepoId = repo.id
            this.namespacePath = repo.identify()
            this.virtualStorage = repo.virtualStorage
            this.relativePath = repo.relativePath
        }

        // 先设置 mirror remote，将源仓库 URL 配置为 origin
        val setMirrorResp =
            repositoryStub.setMirror(
                setMirrorRequest {
                    this.repository = repoProto
                    this.remote = sourceCloneUrl
                }
            )
        if (!setMirrorResp.result) {
            logger.error("SetMirror for repo[${repo.id}] failed: ${setMirrorResp.message}")
            return GitTransferResult(
                success = false,
                errorMessage = "SetMirror failed: ${setMirrorResp.message}",
            )
        }
        logger.info("SetMirror for repo[${repo.id}] succeeded, remote=$sourceCloneUrl")

        val user = userProto {
            this.name = operatorName
            this.tenantId = operatorTenantId
        }

        // 使用源端凭证: "sourceUsername@token" -> SM4 加密
        val encryptedCred = SecretUtil.sm4Encrypt("$sourceUsername@$token")

        val request = pullRequest {
            this.repository = repoProto
            this.user = user
            this.encryptedCredentials = encryptedCred
        }

        return try {
            val response = repositoryStub.pull(request)
            if (response.result) {
                logger.info("gRPC Pull for repo[${repo.id}] succeeded")
                GitTransferResult(success = true)
            } else {
                logger.warn("gRPC Pull for repo[${repo.id}] failed: ${response.message}")
                GitTransferResult(success = false, errorMessage = response.message)
            }
        } catch (e: Exception) {
            logger.error("gRPC Pull for repo[${repo.id}] error", e)
            GitTransferResult(success = false, errorMessage = e.message)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GrpcGitTransferService::class.java)
    }
}
