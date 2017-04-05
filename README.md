Akka Stream Contrib
===================

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akka/akka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Latest version](https://index.scala-lang.org/akka/akka-stream-contrib/akka-stream-contrib/latest.svg)](https://index.scala-lang.org/akka/akka-stream-contrib/akka-stream-contrib) [![Build Status](https://travis-ci.org/akka/akka-stream-contrib.svg?branch=master)](https://travis-ci.org/akka/akka-stream-contrib)

See the [Release page](https://github.com/akka/akka-stream-contrib/releases) for sbt links and release notes.

This project provides a home to Akka Streams add-ons which does not fit into the core Akka Streams module. There can be several reasons for it to not be included in the core module, such as:

* the functionality is not considered to match the purpose of the core module
* it is an experiment or requires iterations with user feedback before including into the stable core module
* it requires a faster release cycle

Caveat Emptor
-------------

A component in this project does not have to obey the rule of staying binary compatible between releases. Breaking API changes may be introduced without notice as we refine and simplify based on your feedback. A module may be dropped in any release without prior deprecation. The Lightbend subscription does not cover support for these modules.
