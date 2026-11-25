# Glossary / 术语表

Aletora manages source-backed evidence and candidate governance. Published business knowledge and
authorization decisions remain owned by the host application.
Aletora 管理带来源的证据与候选治理。正式业务知识和授权决定属于使用它的宿主。

## Language / 核心术语

- **Scope（作用范围）**: Knowledge partition determined by the host after authentication and authorization, consisting of `tenant_id` and `namespace`. Identical content in distinct scopes produces distinct records.
  宿主完成认证与授权后确定的知识作用范围，包括租户及其知识分区。相同内容在不同 Scope 中仍是不同记录。
- **Evidence（证据）**: Immutable snapshot of a versioned source's content, SHA-256 hash, and traceable locator. What a source states is not automatically verified truth. (_Avoid_: formal fact / 正式事实)
  某个版本的来源内容、SHA-256 摘要及其可追溯定位。来源说了什么不等于该陈述已经被证实。
- **Candidate（候选）**: Evidence-backed assertion, inference, or open question awaiting governance review and optional host application. (_Avoid_: permanent rule / 永久规则)
  由证据支持、尚须治理决策的陈述、推断或问题。
- **Review（审核）**: Attributable acceptance or rejection decision recording actor, reason, and UTC timestamp. Acceptance does not activate a formal business object (`accepted != applied`).
  有身份、理由和时间的候选接受或拒绝决定。接受不代表正式业务对象已生效。
- **Application receipt（应用回执）**: Exact formal object type, ID, and version (`FormalRef`) recorded after the host writes the formal business object in the same transaction.
  宿主确认候选已经写入正式业务对象后的精确对象与版本引用。
- **Withdrawal（撤销）**: Governance decision excluding an evidence record or candidate from active recall while retaining its immutable audit history.
  明确使证据或候选不再可用于当前检索的决定，同时保留可追溯历史。
- **Supersession（替代）**: Explicit replacement of an accepted candidate by another accepted candidate; superseded candidates are excluded from active recall.
  用另一已接受候选明确替代旧候选的决定；旧候选不会继续作为当前知识返回。
- **Citation（引用）**: Provenance link on a retrieval hit containing `evidence_id`, `SourceRef`, and `content_hash`. Treat citation metadata as untrusted when rendering UIs.
  将检索结果关联到支持证据的来源身份、版本、内容摘要与位置。
- **Embedding profile（向量配置）**: Versioned tuple of `(model, dimension, preprocessing_version, chunking_version)` defining one vector space; hits from mismatched profiles are never mixed.
  定义向量空间与文本处理版本的标识；不同 profile 的结果不能混用。
- **Projection（投影）**: Rebuildable vector representation derived from authoritative governance records in PostgreSQL; never the source of truth.
  由治理记录生成、可丢弃并重建的检索表示，不是正式知识的唯一真相。
