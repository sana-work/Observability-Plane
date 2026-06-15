#!/usr/bin/env bash
# Phase 0 / Task 0.6 — payload archive bucket: SSE-KMS, lifecycle, public-access-block, prefixes.
set -euo pipefail
ENV="${ENV:?set ENV=dev|staging|prod}"
KMS_KEY="${KMS_KEY:?set KMS_KEY=<kms key id/arn>}"
REGION="${AWS_REGION:-us-east-1}"
BUCKET="ai-obs-payloads-$ENV"
DIR="$(cd "$(dirname "$0")" && pwd)"

aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
  $([ "$REGION" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$REGION") || true

aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"$KMS_KEY\"}}]}"

aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration "file://$DIR/lifecycle.json"

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

for p in redacted-prompts redacted-responses raw-traces rag-contexts \
         audit-evidence debug-bundles rca-reports iac-dashboards; do
  aws s3api put-object --bucket "$BUCKET" --key "$p/"
done

echo "bucket $BUCKET ready (KMS, lifecycle, public-access-block, 8 prefixes)."
