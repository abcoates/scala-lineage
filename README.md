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
multi-threading of process block execution.  For that reason, all result values are expected to be wrapped in a Scala
'[Try](http://www.scala-lang.org/api/current/#scala.util.Try)'
('Success' or 'Failure').  The result of one process block can be used as a parameter for another process block, so
parameter values are **also** expected to be wrapped in a Scala 'Try'.

All parameter and result values must be immutable values, in order to avoid issues the the multi-threaded calculation
of result values.  The library is not able to check that parameters and results are immutable, so it is the
responsibility of users to ensure immutability.  A recommendation is to use Scala
'[Anyval](http://www.scala-lang.org/api/current/#scala.AnyVal)'
values and/or Strings and/or
'[immutable](http://www.scala-lang.org/api/current/#scala.collection.immutable.package)' collections,
possibly in conjunction with Scala case classes.

If a process block has a 'Failure' value for one of its parameters, then it must return a 'Failure' value for every
result value that depends on that parameter value.  In the case of a parameter that is a 'Future', the success/failure
status isn't known until the future completes, so a process block should endeavour to return a 'Future' for any result
that depends on a parameter value that is an uncompleted future.  If the process block is unable to return a future
for a result, it should
[wait for the necessary parameter values to complete](http://www.scala-lang.org/api/current/#scala.concurrent.Await$),
up to a reasonable time-out.  If there is a time-out while waiting for a parameter, the result must be a 'Failure'
value.