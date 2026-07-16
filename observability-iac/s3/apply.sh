#!/usr/bin/env bash
# S3 archive bucket for the Observability Plane.
#   BUCKET=ai-obs-archive-prod AWS_REGION=us-east-1 KMS_KEY_ID=alias/ai-obs ./apply.sh
# Idempotent: create-bucket tolerates AlreadyOwnedByYou; puts re-apply cleanly.
set -euo pipefail
BUCKET="${BUCKET:?set BUCKET, e.g. ai-obs-archive-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
KMS_KEY_ID="${KMS_KEY_ID:-}"

echo "== bucket"
aws s3api create-bucket --bucket "$BUCKET" --region "$AWS_REGION" \
  $( [[ "$AWS_REGION" != "us-east-1" ]] && echo --create-bucket-configuration LocationConstraint="$AWS_REGION" ) \
  2>/dev/null || echo "   (exists)"

echo "== block all public access"
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

echo "== versioning (audit-evidence immutability support)"
aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Enabled

echo "== SSE-KMS default encryption"
if [[ -n "$KMS_KEY_ID" ]]; then
  aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration "{
    \"Rules\": [{\"ApplyServerSideEncryptionByDefault\":
      {\"SSEAlgorithm\": \"aws:kms\", \"KMSMasterKeyID\": \"$KMS_KEY_ID\"},
      \"BucketKeyEnabled\": true}]}"
else
  echo "   KMS_KEY_ID not set — falling back to SSE-S3 (dev only!)"
  aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
    '{"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}'
fi

echo "== lifecycle tiering"
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration "file://$(dirname "$0")/lifecycle.json"

echo "== prefix skeleton"
for p in redacted-prompts redacted-responses raw-traces rag-contexts uploaded-documents \
         audit-evidence debug-bundles rca-reports iac-dashboards; do
  aws s3api put-object --bucket "$BUCKET" --key "$p/.keep" --content-length 0 >/dev/null
  echo "   $p/"
done
echo "== done: s3://$BUCKET"
