# Agent instruction history

Text moved out of `AGENTS.md` and `CLAUDE.md` on October 28, 2026, when process was sized to
the risk of each change. Kept byte-for-byte as historical record; it is not current instruction.

## From AGENTS.md "What this is" (current-state narrative, as of September 30, 2026)

**Current state:** C1 (streaming spine), C2 (initial snapshot), C3 (batching),
C4a/C4b (type breadth + compressed-transaction consumption), the C1a/C1b
position-ownership/start-position rows, C2a (collation-ordered string PKs),
C2b/C2c (`tables: :all` resolution + zero-row completion), XA `:track`, and C7
(**LANDED**: ADR-0013 Accepted — the six-variable gate, the failover-aware cycle
reset, Aurora as a named source verified on a real cluster —
`test/receipts/aurora-mysql-2026-09-30.md` — with the `:aurora_mysql` cluster tier
and the local `:aurora_sim` simulator tier, the latter green in CI on every push).
**1.3.1 is the latest release** (1.3.0 carried the Aurora work but shipped a
first-boot-broken aurora-sim compose; 1.3.1 fixes it — never pin 1.3.0; 1.1.1
carried C1+C2; the 1.2.0 span is additive; 1.2.1–1.2.3 are test-suite and docs
hardening — see CHANGELOG). `examples/replication_pipeline/` is the durable reference
docker stack (repo-side, CI-gated as the public sink API canary). Omitting
`:snapshot` preserves the C1 stream. C6 remains the generic sink-consumer conformance work definition;
the historical roadmap definitions do not reopen the delivered C5 and follow-up mechanisms. The rules below are binding invariants for the
landed code and future rows, grounded in live MySQL probes under `probe/`.
`docs/ROADMAP.md` carries authored scope and acceptance definitions. Current work
pickup uses the GitHub owner queue configured in `.kimosabe/config.toml`
(`baselabs/capstan`); reconcile existing work and receipts before adding tasks.
Historical `.forge/` plans are not the current queue.

## From AGENTS.md "Local substrate" (Compose removal)

This repository ships no database container (the Compose file and its wrapper were removed
September 28, 2026, after agent sessions kept starting databases outside the cluster). On the
maintainer's machine, if a server is unreachable, stop and report it; never start a local MySQL
and never move a port. Elsewhere, any MySQL 8.0/8.4 pair with the same flags, credentials and init
script works (`docs/testing.md` gives the `docker run` lines).

## From AGENTS.md "Docs & lifecycle-artifact policy" (machine-local coordination)

- **Machine-local coordination:** `.kimosabe/` holds local configuration, memory, and
  evidence under the repository's local exclusion policy. Historical `.forge/` artifacts
  remain historical evidence, not current task state; neither is a bare-clone dependency.
  `docs/ROADMAP.md` carries authored scope/acceptance. The configured GitHub owner
  queue and actual receipts govern execution/completion; `forge-roadmap.py` is retired
  as the current pickup rule.

## From AGENTS.md "Development workflow" (whole-repo gate, superseded October 28, 2026)

All gates must pass before a commit. `mix quality` and `mix audit` are defined in `mix.exs`.

## From CLAUDE.md "Quick pointers" (scope and coordination)

- **Scope / pickup:** [`docs/ROADMAP.md`](docs/ROADMAP.md) defines authored scope and
  acceptance. Current executable work uses the GitHub owner queue selected by
  `.kimosabe/config.toml` (`baselabs/capstan`); reconcile existing work and receipts
  before adding tasks. The roadmap does not assert completion.
- **Local coordination:** `.kimosabe/` holds local memory/evidence/configuration.
  `.forge/` plans and derived status are historical, not the current work queue.

## AWS Agent Toolkit rules block (was in both AGENTS.md and CLAUDE.md; generic, not repo-specific)

<!-- BEGIN AWS Agent Toolkit rules (local, uncommitted — new AWS experience) -->
# AWS Guidance for the new AWS experience

This user has signed up for the new AWS experience. This experience lets you sign into AWS using a social provider and requires the following additional context.

Where this guidance conflicts with the project's own instructions, the project's instructions take precedence.

## Context

### Terminology:

- Say "project" instead of "account" — a project contains an AWS account and settings for sharing with other collaborators
- Say "team member" instead of "IAM user" — users are invited by email, not created or federated in IAM
- Say "AWS Settings" when referring to management tasks at [settings.aws.com](https://settings.aws.com/) (project management, billing, team members, spend limits). Users view their actual AWS resources in the AWS Management Console.
- Say "selected Region" when referring to the user's Region — not "home Region"
- The user has a managed IAM experience. This includes a managed service control policies (SCP) and resource control policies (RCP) that govern the use of AWS. They will still need to use IAM to create policies to let services work with each other. If there are questions about the SCPs or RCPs, go to the documentation at https://docs.aws.amazon.com/accounts/latest/reference/scps-and-rcps-for-projects.html

### Constraints:

- All projects share a single AWS Region determined by the user's contact address. Resources cannot be created in other Regions
- When developing:
  - MUST create all Regional resources in the project's assigned Region
  - You CAN create AWS WAF and Cloudwatch Logs resources in us-east-1 when there are global resources (like a global WAF instance) that require a connection to dependencies in us-east-1. You should not use these for any other reason, because resources in the selected Region will provide lower cost (due to no cross-Region traffic), increased availability (due to no cross-Region traffic), and easier manageability (due to not needing to look in another Region). When you need to do an inventory of resources, you need to look in both the selected Region and us-east-1 for Cloudwatch Logs or WAF resources.
  - MUST NOT attempt to create Lambda, API Gateway, or other Regional resources in any other Region
  - MUST direct users to confirm their Region in AWS Settings > View all projects > Overview > Additional Info > Region. If the user cannot confirm their Region, check in ~/.aws/config
  - MUST NOT use Lambda@Edge — excluded from both Lambda and CloudFront
  - MUST NOT use CloudFormation StackSets — no multi-account or multi-Region deployments
  - MUST NOT attempt cross-Region actions — no cross-Region replication for DynamoDB/S3/RDS, no multi-Region KMS keys
  - MUST NOT use Route 53 cross-Region routing — geolocation, latency-based, and failover routing policies are not available
  - CloudFront is a global service and its actions ARE allowed in `us-east-1`. A user can create a CloudFront distribution pointing to their project-region Lambda function URL or API Gateway. However, Lambda and API Gateway themselves MUST NOT be created in `us-east-1` — they must be in the project Region.
  - Reduced availability in `eu-north-1` specifically: Amazon Rekognition, Amazon Textract, Amazon Personalize, AWS App Runner are not available in that Region.
- IAM permissions for human access are managed by AWS. Don't assign roles to team members unless absolutely necessary
- The user may have a spend limit if they are on the paid plan. The limit that pauses their project if it's exceeded. If resources suddenly become inaccessible, ask if they have a spend limit configured. Only project owners can modify a spend limit.
- When developing:
  - MUST ask about spend limit status if the user reports sudden "Access Denied" errors on operations that previously worked
  - MUST direct users to check spend status in AWS Settings > Billing
  - MUST check if a user has upgraded their account to the paid plan
  - MUST ask the user if they want to clean up the successfully created resources or keep them to reduce cost
- The user sets up billing, creates spend limits, and retrieves and pays invoices in AWS Settings. The user creates budgets and optimizes their costs in the AWS Billing and Cost Management console
- Not all AWS services are available. If a service isn't working, do the following:
  1. Run the command `aws freetier get-account-plan-state`
  2. If accountPlanType": "FREE", check the [Free Tier supported services list](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html#supported-services-free-tier) next,
  3. If accountPlanType": "PAID", check the [Paid Tier supported services list](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html#supported-services-paid-plan).
  4. If neither list shows the service, check the [Not supported for this experience list](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html#unsupported-services). The user will need to activate advanced features to access this service.
- Users can activate advanced AWS services and capabilities for their account.
- Before starting a task, check whether a relevant AWS skill is available. Load the skill with retrieve_skill and prefer its guidance over general knowledge.

### Help level

- help_level (required): LOW, MEDIUM, or HIGH. While a user is building, you MUST ask the user: "How much guidance would you like from me? Low (I only flag security risks), medium (I ask a couple of clarifying questions if something seems off), or high (I explain what I'm doing, suggest alternatives, and flag best practices)."

You CAN update this rule file to save a user's help_level.

Constraints for each level:

**LOW:**

- MUST follow all constraints in this context file
- MUST execute the user’s request without modification
- MUST NOT ask clarifying questions unless the action would create a security vulnerability
- MUST NOT suggest alternatives or improvements

**MEDIUM:**

- MUST execute the user's request
- MAY ask up to two clarifying questions per task if the request has an ambiguity or a potential issue
- MUST NOT repeat a question or suggestion the user has already dismissed
- MUST NOT explain trade-offs or alternatives unless the user asks

**HIGH:**

- MUST explain what each step does and why before executing it
- MUST suggest alternatives when a better approach exists
- MUST flag best practices and explain trade-offs
- MUST still execute the user's choice if they disagree with a suggestion

<!-- END AWS Agent Toolkit rules -->

## From AGENTS.md Critical Rule 5 (original text; its compressed-transaction halt sentence is superseded by Rule 2 and the zstd decoder)

**5. Transaction shape is explicit; unknown shapes halt (design Q13).** Three terminators close a
transaction: `XID`; `QUERY("COMMIT")` (non-transactional engines); and the **self-committing DDL
`QUERY`** (`GTID → QUERY(DDL)` with no `BEGIN`/`XID`, live-verified) which advances the checkpoint and
yields `%SchemaChange{}`. **`XA_PREPARE_LOG_EVENT` halts `:unsupported_transaction_shape`
under the default `xa: :refuse` — its rows must NEVER be delivered** (they may later
roll back). The opt-in `xa: :track` (ADR-0006) replaces the halt with prepare-pool
tracking under the held-out watermark: a prepare GTID checkpoints ONLY in the same
write as its resolution GTID; XID bytes are pooled under a sha256 key and never
emitted (Rule 1). Compressed transactions
(`TRANSACTION_PAYLOAD_EVENT`) halt loudly. Any unknown event type fails closed, never silently
skipped.
