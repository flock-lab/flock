# Copyright (c) 2020-present, UMD Database Group.
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

#!/usr/bin/env sh

set -e

# declare -a log_group_names=(
#     "/aws/lambda/q4-02-00" "/aws/lambda/q4-02-01" "/aws/lambda/q4-02-02"
#     "/aws/lambda/q4-02-03" "/aws/lambda/q4-02-04" "/aws/lambda/q4-02-05"
#     "/aws/lambda/q4-02-06" "/aws/lambda/q4-02-07" "/aws/lambda/q4-02-08"
#     "/aws/lambda/q4-02-09" "/aws/lambda/q4-02-10" "/aws/lambda/q4-02-11"
#     "/aws/lambda/q4-02-12" "/aws/lambda/q4-02-13" "/aws/lambda/q4-02-14"
#     "/aws/lambda/q4-02-15")

# log_group_names is a list from `aws/lambda/q4-03-00` to `aws/lambda/q4-03-15`
declare -a log_group_names=(
    "/aws/lambda/q4-03-00" "/aws/lambda/q4-03-01" "/aws/lambda/q4-03-02"
    "/aws/lambda/q4-03-03" "/aws/lambda/q4-03-04" "/aws/lambda/q4-03-05"
    "/aws/lambda/q4-03-06" "/aws/lambda/q4-03-07" "/aws/lambda/q4-03-08"
    "/aws/lambda/q4-03-09" "/aws/lambda/q4-03-10" "/aws/lambda/q4-03-11"
    "/aws/lambda/q4-03-12" "/aws/lambda/q4-03-13" "/aws/lambda/q4-03-14"
    "/aws/lambda/q4-03-15")

for log_group_name in "${log_group_names[@]}"; do
    log_stream_names=$(aws logs describe-log-streams \
        --log-group-name "$log_group_name" \
        --order-by LastEventTime \
        --descending \
        --max-items 1 \
        --query 'logStreams[*].logStreamName' \
        --output text)

    # Remove `None` from the list of log stream names
    log_stream_names=$(echo "$log_stream_names" | grep -v None)

    for log_stream_name in $log_stream_names; do
        aws logs get-log-events \
            --log-group-name "$log_group_name" \
            --log-stream-name "$log_stream_name" \
            --output text
    done > cloudwatch.log

    # grep -r "Billed Duration" cloudwatch.log | awk '{print $11}'
    printf '%s ' "$log_group_name"
    grep -r "Billed Duration" cloudwatch.log | awk '{print $11}' | awk '{s+=$1} END {printf "billed duration: %.0f\n", s}'
done > billed_duration.log

cat billed_duration.log | awk '{print $4}' | awk '{s+=$1} END {printf "total billed duration: %.0f\n", s}'
