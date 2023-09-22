# Releasing the Connectors project

The version scheme we use is **MAJOR.MINOR.PATCH.BUILD** and stored in the [VERSION](https://github.com/elastic/connectors-python/blob/main/connectors/VERSION) file at the root of this repository.

## Unified release

**MAJOR.MINOR.PATCH** should match the Elastic and Enterprise Search version it targets and the *BUILD* number should be set to **0** the day the Connectors release is created to be included with the Enterprise Search distribution.

For example, when shipping for `8.1.2`, the version is `8.1.2.0`.

To release Connectors:

1. Make sure all tests and linter pass with `make lint test`
2. Run `make release`
3. Set the [VERSION](../connectors/VERSION) file to the new/incremented version on the release branch
4. PR this change to the appropriate Connectors release branch

A package will be generated in `dist/`

Take care of the branching (minor releases only):

- Increment the VERSION on main to match the next minor release
- Create a new maintenance branch
- Make sure the `.backportrc.json` is updated. The previous minor is added to `targetBranchChoices` and the new minor is used in `branchLabelMapping`

After the Elastic unified release is complete

- Update the **BUILD** version ([example PR](https://github.com/elastic/connectors-python/pull/122)). Note that the Connectors project does not immediately bump to the next **PATCH** version. That won't happen until that patch release's FF date.

## Releasing docker images

To release the docker image, follow these steps:

1. Make sure you're on the right tagged release commit, e.g. `v8.10.2.0`
   - If no release tag exists yet, use the release branch, e.g. `8.10`
2. Make sure the version in [VERSION](../connectors/VERSION) is correct
3. Run `docker login -u <username> -p <password> docker.elastic.co` with credentials that allow for release
4. Edit the [Makefile](../Makefile) to remove `-SNAPSHOT` from the `docker-build`, `docker-run` and `docker-push` steps
5. Run `make docker-build`
6. Run `make docker-run` to check that the docker image runs correctly
    - If the image runs and complains about not finding elasticsearch on port 9200, that's okay -- this is enough to confirm the image works
7. Run `make docker-push` to complete the release

## In-Between releases

Sometimes, we need to release Connectors independently from Enterprise Search. For instance, if someone wants to use the project as an HTTP Service, and we have a bug fix we want them to have as soon as possible.

In that case, we increment the **BUILD** number, and follow the same release process than for the unified release.

So `8.1.2.1`, `8.1.2.2` etc. On the next unified release, the version will be bumped to the next **PATCH** value, and **BUILD** set to `0`

**In-Between releases should never introduce new features since they will eventually be merged into the next PATCH release. New features are always done in Developer previews**

## Developer preview releases

For developer previews, we are adding a `pre` tag using an ISO8601 date.
You can use `make release_dev` instead of `make release` in that case.
