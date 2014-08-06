scala-lineage
=============

'scala-lineage' is a library that supports self-describing processing pipelines that allow tracking of data lineage
from inputs through intermediate results to outputs.

The core class is 'ProcessBlock', which represents all or part of a processing pipeline.  A process block can be
composed of other process blocks.

A process block can have multiple named parameters (inputs) and multiple named results (outputs).  These are handled
using the 'ParameterMap' and 'ResultMap' classes respectively.

'scala-lineage' supports and encourages the use of Scala
'[Futures](http://www.scala-lang.org/api/current/index.html#scala.concurrent.Future)' to provide automated
multi-threading of process block execution.