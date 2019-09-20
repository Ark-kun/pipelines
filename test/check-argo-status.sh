#!/bin/bash
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

ARTIFACT_DIR=$WORKSPACE/_artifacts
WORKFLOW_COMPLETE_KEYWORD="completed=true"
WORKFLOW_FAILED_KEYWORD="phase=Failed"
PULL_ARGO_WORKFLOW_STATUS_MAX_ATTEMPT=$(expr $TIMEOUT_SECONDS / 20 )

echo "check status of argo workflow $ARGO_WORKFLOW...."
# probing the argo workflow status until it completed. Timeout after 30 minutes
for i in $(seq 1 ${PULL_ARGO_WORKFLOW_STATUS_MAX_ATTEMPT})
do
  WORKFLOW_STATUS=`kubectl get workflow $ARGO_WORKFLOW -n ${NAMESPACE} --show-labels 2>&1` \
    || echo kubectl get workflow failed with "$WORKFLOW_STATUS" # Tolerate temporary network failure during kubectl get workflow
  echo $WORKFLOW_STATUS | grep ${WORKFLOW_COMPLETE_KEYWORD} && s=0 && break || s=$? && printf "Workflow ${ARGO_WORKFLOW} is not finished.\n${WORKFLOW_STATUS}\nSleep for 20 seconds...\n" && sleep 20
done

if [[ "$s" == 0 ]]; then
  echo "Argo workflow finished."
  if [[ -n "$TEST_RESULT_FOLDER" ]]; then
    echo "Copy test result"
    mkdir -p "$ARTIFACT_DIR"
    gsutil cp -r "${TEST_RESULTS_GCS_DIR}"/* "${ARTIFACT_DIR}" || true
  fi
  argo get "${ARGO_WORKFLOW}" -n "${NAMESPACE}"
  exit 0
fi

# Handling failed workflow
if [[ "$s" != 0 ]]; then
  echo "Argo workflow timed out."
else
  echo "Argo workflow failed."
fi

echo "=========Argo Workflow Logs========="
argo logs -w "${ARGO_WORKFLOW}" -n "${NAMESPACE}"

echo "========All workflows============="

argo --namespace "${NAMESPACE}" list --output=name |
  while read workflow_id; do
    echo "========${workflow_id}============="
    argo get "${workflow_id}" -n "${NAMESPACE}"
  done

echo "=========Main workflow=============="
argo get "${ARGO_WORKFLOW}" -n "${NAMESPACE}"

exit 1
