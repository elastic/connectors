## Here we define our Buildkite pipelines

We use our own custom image. The image definition can be found here: https://github.com/elastic/ci-agent-images/pull/132

The image is built weekly, see the cron definition: https://github.com/elastic/ci/pull/1813/files

The image and cron job were built following instruction from several sources:

- https://docs.elastic.dev/ci/agent-images-for-buildkite
- https://github.com/elastic/ci/blob/main/vm-images/README.md
- https://github.com/elastic/ci-agent-images/README.md

In case something is unclear, don't hesitate to contact #buildkite Slack channel.
