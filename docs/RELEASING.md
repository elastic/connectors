# Releasing the Connectors project

In versions 8.15 and earlier, the versioning scheme we used was **MAJOR.MINOR.PATCH.BUILD**.

Starting in 8.16, we began using semantic versioning aligned with the rest of the Elastic stack (**MAJOR.MINOR.PATCH**).

The version is stored in the [VERSION](https://github.com/elastic/connectors/blob/main/connectors/VERSION) file at the root of this repository.

## After Feature Freeze
Take care of the branching (minor releases only):

- Increment the VERSION on main to match the next minor release
- Create a new maintenance branch
- Make sure the `.backportrc.json` is updated. The previous minor is added to `targetBranchChoices` and the new minor is used in `branchLabelMapping`


## Unified release, (>= 8.16)

The VERSION file should match that of the Elastic stack.

On the day of the Feature Freeze, `#mission-control` will notifiy the release manager that it's time to create a new maintenance branch.

On the day of the release, `#mission-control` will notify the releae manager that it's time to bump the VERSION to the next PATCH.

The Unified Release build will take care of producing git tags and official artifacts from our most recent DRA artifacts.

### In-Between releases

Sometimes, we need to release Connectors independently from the Elastic unified-release.
For instance, if a user reports a critical bug in Connectors, and we want to ship a fix as soon as possible.

In this case, we can work with `#early-agent-releases` to trigger the Independent Agent Release Pipeline ([staging](https://buildkite.com/elastic/independent-agent-release-staging), [releasing](https://buildkite.com/elastic/independent-agent-release-releasing)) once the bugfix has been merged and a staging DRA artifact produced.
This pipeline is used for integrations that need to release more frequently than the stack cadence, but still need to stay associated with the stack versioning scheme.
This will produce Connectors (and Agent) artifacts like **MAJOR.MINOR.PATCH+build<TIMESTAMP>**.
These versions are compatible with SEMVER (Semantic Versioning).
While the pipeline was built by-and-for Agent, it will release all our connector artifacts - not just the agent docker image.

No changes to the VERSION file are necessary for these "in-between" releases.

## For versions <= 8.15

**MAJOR.MINOR.PATCH** should match the Elastic and Enterprise Search version it targets and the *BUILD* number should be set to **0** the day the Connectors release is created to be included with the Enterprise Search distribution.

For example, when shipping for `8.1.2`, the version is `8.1.2.0`.


After the Elastic unified release is complete

- Update the **BUILD** version ([example PR](https://github.com/elastic/connectors/pull/122)). Note that the Connectors project does not immediately bump to the next **PATCH** version. That won't happen until that patch release's FF date.

### Releasing docker images

To release the docker image, follow these steps:

1. Make sure that you're on the right branch and VERSION file is correct: `cat connectors/VERSION`. The value in this file should be your _expected_ release version - e.g. if you're releasing 8.12.2.1, then VERSION should also be 8.12.2.1.
2. Go to https://buildkite.com/elastic, find "connectors-docker-build-publish" pipeline and trigger a Build:
  - Click on "New Build"
  - Enter a descriptive message, choose commit (or leave HEAD if you build the last commit of the branch) and enter the branch (in the example with 8.12.2.1 you would put `8.12` here)
  - Press "Create Build" and wait for the build to finish
3. Tag the commit that was used for the image with `git tag <tag_id> && git push origin <tag_id>`. For the example above, tag_id would be `v8.12.2.1`
4. Update `connectors/VERSION` file and bump the last part of the version by an increment and submit a PR with it. In the example above, the value would be `8.12.2.2`.

### In-Between releases

Sometimes, we need to release Connectors independently from Enterprise Search. For instance, if someone wants to use the project as an HTTP Service, and we have a bug fix we want them to have as soon as possible.

In that case, we increment the **BUILD** number, and follow the same release process than for the unified release.

So `8.1.2.1`, `8.1.2.2` etc. On the next unified release, the version will be bumped to the next **PATCH** value, and **BUILD** set to `0`

**In-Between releases should never introduce new features since they will eventually be merged into the next PATCH release.**


