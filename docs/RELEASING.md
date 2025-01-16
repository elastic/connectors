# Releasing the Connectors project

In versions 8.15 and earlier, the versioning scheme we used was **MAJOR.MINOR.PATCH.BUILD**.

Starting in 8.16, we began using semantic versioning aligned with the rest of the Elastic stack (**MAJOR.MINOR.PATCH**).

The version is stored in the [VERSION](https://github.com/elastic/connectors/blob/main/connectors/VERSION) file at the root of this repository.

## After Feature Freeze
Take care of the branching (minor releases only):

- Increment the VERSION on main to match the next minor release
- Create a new maintenance branch
- Make sure `.backportrc.json` is updated: the previous minor is added to `targetBranchChoices` and the new minor is used in `branchLabelMapping`
- Make sure `renovate.json` is updated: the previous minor is added to `labels` (for example, `v8.18`). [Create that label](https://github.com/elastic/connectors/labels) if it doesn't exist yet


## Unified release, (>= 8.16)

The VERSION file should match that of the Elastic stack.

On the day of the Feature Freeze, `#mission-control` will notify the release manager that it's time to create a new maintenance branch.

Make sure that the `catalog-info.yml` on the `main` branch is running nightly builds for any new maintenance branch.

On the day of the release, `#mission-control` will notify the release manager that it's time to bump the VERSION to the next PATCH.

The Unified Release build will take care of producing git tags and official artifacts from our most recent DRA artifacts.

### Pre-release artifacts

If `#mission-control` asks for a pre-release artifact to be built, trigger the build pipeline from the relevant branch
and add an Environment Variable for `VERSION_QUALIFIER` with the value of the pre-release.

For example, to release 9.0.0-BC1, you would set `VERSION_QUALIFIER` to be `BC1` for this build.

### In-Between releases

Sometimes, we need to release Connectors independently of the Elastic unified-release.
For instance, if a user reports a critical bug in Connectors, and we want to ship a fix as soon as possible.

In this case, we can follow the below process:

1. create a new dev branch from the branch you want to release from. e.g:
    ```
    git checkout 8.16 && git checkout -b seanstory/release-8.16.0-20241108 && git push origin seanstory/release-8.16.0-20241108
    ```
2. Go to https://buildkite.com/elastic, find "connectors-docker-build-publish" pipeline and trigger a Build:
   - Choose the dev branch you just created. In this example, `seanstory/release-8.16.0-20241108`
   - Click on "New Build"
   - Enter a descriptive message, and leave HEAD as the commit
   - By default the docker image will be pushed to namespace `integrations`. To push the image to `enterprise-search`, add environment variable `DOCKER_IMAGE_NAME=docker.elastic.co/enterprise-search/elastic-connectors`.
   - Press "Create Build" and wait for the build to finish
3. The build will have created a few commits and a git tag. 
  To make sure the commits are visible in the right places, you now need to make a PR from your dev branch to the maintenance branch.
  In this example: `8.16 <- seanstory/release-8.16.0-20241108`.
  Note, the PR might have an empty diff. This is ok.
4. Once that PR is merged, you're done.

This will produce Connectors artifacts like **MAJOR.MINOR.PATCH+build<TIMESTAMP>**.
These versions are compatible with SEMVER (Semantic Versioning).
This will automatically create the necessary git tag.

Note that `PATCH` will be 1 less than the current contents of the VERSION file.
This is because the VERSION file is usually bumped right after a stack release.
So right after 8.16.0, the VERSION file is bumped to 8.16.1.
If we want to release again the next day, the in-between artifacts should be associated with 8.16.0 stack versions, not with 8.16.1.

No manual changes to the VERSION file are necessary for these "in-between" releases.
During the pipeline, it will bump the VERSION file in the branch, build the artifacts, then restore the VERSION file.
If the release build fails for some reason, check to make sure the VERSION file was properly restored.

