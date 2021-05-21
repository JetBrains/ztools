# ZTools

[![JetBrains incubator project](https://jb.gg/badges/incubator.svg)](https://confluence.jetbrains.com/display/ALL/JetBrains+on+GitHub)

Wrapper for scala interpreter for use with Apache Zeppelin server to provide values of variables between paragraphs execution.

Used to provide variable view functionality for Jetbrains Big Data Tools plugin.  

## How it works

The library allows us to access the current state of scala REPL and save the values of variables as JSON.

Some computations may be performed for some types to save state that is not accessible readily as a set of fields (for example, this is the case for the number of partitions for RDD).

These values can then be of use, in particular in Apache Zeppelin integration for Big Data Tools plugin we can show the collected values in variables view panel - an alternative to debugger evaluation in spark runtime.

## Usage

To use ZTools with Big Data Tools you need to do 2 things.

1. In Zeppelin connection settings in BDT plugin please click to `Enable ZTools Integration (Experimental)` and save settings.

2. You need to install the ZTools jar to the Zeppelin server. You can do it in auto and manual ways. The automatic way is to click to `Install ZTools...` in a bubble that appears after enabling ZTools integration in settings. The library will be download from maven repo `https://packages.jetbrains.team/maven/p/bdt/bigdatatools/`. During this operation, the repository and the dependency on the Zeppelin interpreter will be added.

If you do not have the permissions to add repositories and dependencies to Zeppelin interpreters you need to ask the administrators to do it manually.
You can add dependency as a file jar (https://packages.jetbrains.team/maven/p/bdt/bigdatatools/org/jetbrains/ztools/ztools-spark-all/0.212.1/ztools-spark-all-0.212.1.jar) or add to Zeppelin maven repo (`https://packages.jetbrains.team/maven/p/bdt/bigdatatools`) and maven artifact (`org.jetbrains.ztools:ztools-spark-all:0.212.1`).

After install, you need to run any paragraph in a Zeppelin notebook to collect data from the server.

For stand-alone usage see `org.jetbrains.ztools.spark.Tools.init` method for initialization and `Tools.getEnv` to get current state.

## Resources

If you need support: [Our Slack channel](https://slack-bdt.mau.jetbrains.com/?_ga=2.181253743.913531920.1594027385-1936946878.1588841666)

Apache Zeppelin (https://zeppelin.apache.org/)

Big Data Tools (https://plugins.jetbrains.com/plugin/12494-big-data-tools)

## License
[Apache v2](LICENSE.txt).
