# Upgrading

The `connectors` project is intentionally de-coupled from the Elasticsearch release cycle.
It can be forked, modified, and connected to an Elasticsearch deployment, and continue to be modified while the Elasticsearch version stays static.

This gives the product a great capability to adapt to bugs or sudden 3rd-party API changes flexibly and without delay.

However, it can also lead to confusion when eventually upgrading if the elastic-maintained remote and your fork have significantly diverged.

In order to protect yourself from undue difficulty in upgrading, follow the below simple rules on your fork.

### **ALWAYS TAKE AN [ELASTICSEARCH SNAPSHOT](https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-restore.html) FIRST!**
Too dramatic? No.

Nearly all upgrades-gone-wrong can be fixed on a second attempt.
But if you do not snapshot your Elasticsearch data before a bad upgrade, it is VERY difficult to make a second attempt.
Do not ignore this step.
Do not tell yourself it is overkill.

Always. 👏 Snapshot. 👏 First 👏.

### Always read release notes
The `connectors` repository is part of Elastic Enterprise Search's connectors framework.
You can find the [release notes for Enterprise Search here](https://www.elastic.co/guide/en/enterprise-search/current/changelog.html).
You can find the list of [known issues for connectors here](https://www.elastic.co/guide/en/enterprise-search/current/connectors-known-issues.html).

### Limit changes to your own connectors
This means, rather than modifying an existing data source, it is often better to copy-and-rename a data source (and its tests!), and modify the new copy.
This allows you to pull changes into your fork without risking merge conflicts, and allows you to then apply any changes as you see fit.

### Avoid framework changes
As Elastic adds new features to the framework, internal classes are renamed, moved, and their method signatures change.
Periodically, the [connector protocol](./CONNECTOR_PROTOCOL.md) changes.
If you find a change that needs to be made at the framework level, [submit a PR](./CONTRIBUTING.md#pull-request-etiquette) for the fix, so that your branch does not differ from the Elastic-maintained remote.

### Upgrade all stack components beforehand
Before upgrading `connectors`, you should first stop your running connectors services, upgrade-and-start Elasticsearch, upgrade-and-start Enterprise Search, upgrade-and-start Kibana, and only then upgrade-and-start `connectors`.
As a part of Enterprise Search, the connectors framework's data migrations live inside Enterprise Search.
The user experience for configuring and running the connectors framework lives inside Kibana.
Both of these store their state in Elasticsearch.
Upgrading-and-starting your `connectors` _before_ any of these stack components can lead to unexplained behavior, and may result in data corruption.

If anything goes wrong here, hopefully you [took a snapshot](#always-take-an-elasticsearch-snapshothttpswwwelasticcoguideenelasticsearchreferencecurrentsnapshot-restorehtml), and you can roll back, trying again with the correct order.

### Ask for help if you need it
We want you to be successful!
If you need help, you can reach out for [official support services](./SUPPORT.md#official-support-services) or can [reach out to the development team](./SUPPORT.md#where-else-can-i-go-to-get-help).

### Upgrading from versions <= 8.7
The connectors framework became GA (Generally Available) in 8.8.0.
Upgrades are expected to be seamless and are fully supported on GA versions.

Before the connectors framework became GA, we still attempted to make upgrading easy, but some beta features may require manual re-configuration.

If you believe you spot a bug or issue, please [report it](./CONTRIBUTING.md#reporting-issues).