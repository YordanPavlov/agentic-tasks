# Existing situation
Inside our devops repo at ~/santiment/src/devops we use the Flink operator for our new Flink deploys. Key directories to check are: template live in ~/santiment/src/devops/global/flink-jobs-operator and Hetzner production deploys live in ~/santiment/src/devops/hprod/k8s-apps/flink-jobs-operator/. Feel free to also inspect the operator logs (deployed in Hetzner production, use the kubectl skill with the hprod cluster).

# Task goal
Our goal is to go over the Flink project release documents and figure out if any of the newer Flink operator versions has a bugfix or new functionality which is relevant for us. This task is about the Flink Kubernetes Operator only, not Flink core (or CDC, Agents, etc.). Flink release announcements are listed at https://flink.apache.org/posts/ (paginated, newest first); the same posts are available as an RSS feed at https://flink.apache.org/index.xml, which is easier to filter for "Kubernetes Operator" announcements. Check what became released following the version of the operator we have deployed.

Before starting, read log.md to find the last operator version already reviewed, and only cover releases newer than both that version and the deployed one.

# Task operation
On each run you do, you need to output what changed in this release along with your explanation of it. You should emphasise a feature or bugfix if it is relevant for us. Feel free to search the internet to better understand new functionality yourself before explaining. Once you are done with your analysis and output you do a short record in the session log which lives in the log.md file inside the current directory. Each record contains the date, the deployed operator version, the newest version reviewed, and the FLINK-XXXXX IDs of the items you found relevant.
