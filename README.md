# ZTools

[![JetBrains incubator project](https://jb.gg/badges/incubator.svg)](https://confluence.jetbrains.com/display/ALL/JetBrains+on+GitHub)

Wrapper for scala interpreter for use with Apache Zeppelin server to provide values of variables between paragraphs execution.

Used to provide variable view functionality for Jetbrains Big Data Tools plugin.  

## How it works

The library allows to access current state of scala REPL and save values of variables as JSON.

Some computations may be performed for some types to save state that is not accessible readily as a set of fields (for example, this is the case for number of partitions for RDD).

These values can then be of use, in particular in Apache Zeppelin integration for Big Data Tools plugin we can show the collected values in variables view panel - an alternative to debugger evaluation in spark runtime.

## Usage

To use ZTools with Big Data Tools, please enable 'Enable ZTools integration' option in your Zeppelin connection settings.

Then, either use pop-up hint to download the `ztools-spark-all` jar from bintray artifactory or manually add the jar to spark interpreter dependencies.

For stand-alone usage see `org.jetbrains.ztools.spark.Tools.init` method for initialization and `Tools.getEnv` to get current state.

## Resources

Apache Zeppelin (https://zeppelin.apache.org/)

Big Data Tools (https://plugins.jetbrains.com/plugin/12494-big-data-tools)

## License
[Apache v2](LICENSE.txt).