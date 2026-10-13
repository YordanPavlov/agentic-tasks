# Existing situation
Inside our Flink jobs repo at ~/santiment/src/etherbi-flink we create a deployable image where we merge our own Flink jobs with the Flink releases.

# Task goal
Our goal is to see if a new upstream Flink release has become available and figure out if any of the new functionality or bugfixes are important for us. This task is about the Flink framework core and the Flink connectors we use, not the Flink operator (or CDC, Agents, etc.). Flink release announcements are listed at https://flink.apache.org/posts/ (paginated, newest first); the same posts are available as an RSS feed at https://flink.apache.org/index.xml; use whichever is easier. Connector releases are not always announced there, so check https://flink.apache.org/downloads/ and the connector release notes in the Flink Jira as well.

To find the Flink version in use, inspect Dockerfile and build.sbt inside ~/santiment/src/etherbi-flink; build.sbt also lists the connectors in use and their versions. If the Flink version in the Dockerfile and build.sbt differ, report the mismatch. Check which releases came out after the versions in use. Cover both upgrade paths: newer patch and minor releases of the major version we use, and newer major versions. For newer minor and major versions, point out breaking changes and the migration effort.

Before starting, read log.md to find the last versions already reviewed, and only cover releases newer than both those and the ones in use. If log.md is missing, start from the version in use and create it.

# Task operation
On each run you do, you need to output what changed in each new release along with your explanation of it. You should emphasise a feature or bugfix if it is relevant for us. Judge relevance mainly from our source code (the APIs, connectors and state backend our jobs use); you may inspect live deploys if needed. Feel free to search the internet to better understand new functionality yourself before explaining. Once you are done with your analysis and output you do a short record in the session log which lives in the log.md file inside the current directory. Each record contains the date, the Flink and connector versions in use, the newest versions reviewed, and the FLINK-XXXXX IDs of the items you found relevant.
