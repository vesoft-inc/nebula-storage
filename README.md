<p align="center">
  <img src="docs/logo.png"/>
  <br> English | <a href="README-CN.md">中文</a>
  <br>A distributed, scalable, lightning-fast graph database<br>
</p>
<p align="center">
  <a href="https://hub.docker.com/u/vesoft">
    <img src="https://github.com/vesoft-inc/nebula-storage/workflows/docker/badge.svg" alt="build docker image workflow"/>
  </a>
  <a href="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula star"/>
  </a>
  <a href="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula fork"/>
  </a>
  <a href="https://codecov.io/gh/vesoft-inc/nebula">
    <img src="https://codecov.io/gh/vesoft-inc/nebula/branch/master/graph/badge.svg" alt="codecov"/>
  </a>
  <br>
</p>

# What is Nebula Graph?

**Nebula Graph** is an open-source graph database capable of hosting super large scale graphs with dozens of billions of vertices (nodes) and trillions of edges, with milliseconds of latency.

Compared with other graph database solutions, **Nebula Graph** has the following advantages:

* Symmetrically distributed
* Storage and computing separation
* Horizontal scalability
* Strong data consistency by RAFT protocol
* SQL-like query language
* Role-based access control for higher level security

## Notice of Release

After v2.5.0, we use [Nebula repo](https://github.com/vesoft-inc/nebula).

## Supported Clients
* [Go](https://github.com/vesoft-inc/nebula-go)
* [Python](https://github.com/vesoft-inc/nebula-python)
* [Java](https://github.com/vesoft-inc/nebula-java)

## Quick start

Read the [Getting started](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/1.overview/2.quick-start/1.get-started.md) article for a quick start.

Please note that you must install **Nebula Graph** by [installing source code](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/3.build-develop-and-administration/1.build/1.build-source-code.md), [rpm/deb packages](https://github.com/vesoft-inc/nebula/tree/master/docs/manual-EN/3.build-develop-and-administration/3.deploy-and-administrations/deployment/install-with-rpm-deb.md) or [docker compose](https://github.com/vesoft-inc/nebula-docker-compose), before you can actually start using it. If you prefer a video tutorial, visit our [YouTube channel](https://www.youtube.com/channel/UC73V8q795eSEMxDX4Pvdwmw).

## Getting help

In case you encounter any problems playing around **Nebula Graph**, please reach out for help:

* [Slack channel](https://join.slack.com/t/nebulagraph/shared_invite/enQtNjIzMjQ5MzE2OTQ2LTM0MjY0MWFlODg3ZTNjMjg3YWU5ZGY2NDM5MDhmOGU2OWI5ZWZjZDUwNTExMGIxZTk2ZmQxY2Q2MzM1OWJhMmY#)
* [Stack Overflow](https://stackoverflow.com/search?q=%5Bnebula-graph%5D&mixed=0)
* [Official Forum](https://discuss.nebula-graph.io)

## Documentation

* [English](https://docs.nebula-graph.io/2.0/)
<!--* [简体中文](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-CN/README.md)-->

## Architecture

![image](https://github.com/vesoft-inc/nebula-docs/raw/master/images/Nebula%20Arch.png)

## Contributing

Contributions are warmly welcomed and greatly appreciated. And here are a few ways you can contribute:

* Start by some [good first issues](https://github.com/vesoft-inc/nebula/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
* Submit Pull Requests to us. See [how-to-contribute](docs/manual-EN/4.contributions/how-to-contribute.md).

## Licensing

**Nebula Graph** is under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license, so you can freely download, modify, and deploy the source code to meet your needs. You can also freely deploy **Nebula Graph** as a back-end service to support your SaaS deployment.

In order to prevent cloud providers monetizing from the project without contributing back, we added [Commons Clause 1.0](https://commonsclause.com/) to the project. As mentioned above, we fully commit to the open source community. We would love to hear your thoughts on the licensing model and are willing to make it more suitable for the community.

## Contact

* Twitter: [@NebulaGraph](https://twitter.com/NebulaGraph)
* [Facebook page](https://www.facebook.com/NebulaGraph/)
* [LinkedIn page](https://www.linkedin.com/company/vesoft-nebula-graph/)
* [Slack channel](https://join.slack.com/t/nebulagraph/shared_invite/enQtNjIzMjQ5MzE2OTQ2LTM0MjY0MWFlODg3ZTNjMjg3YWU5ZGY2NDM5MDhmOGU2OWI5ZWZjZDUwNTExMGIxZTk2ZmQxY2Q2MzM1OWJhMmY#)
* email: info@vesoft.com
