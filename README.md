# Cascalog Graph

** WARNING - ALPHA QUALITY, USE AT YOUR OWN RISK, SUBJECT TO BUGS AND API CHANGES **

Cascalog Graph is a library that combines two ideas: [workflow checkpoint](https://github.com/nathanmarz/cascalog-contrib/tree/master/cascalog.checkpoint) and [declarative abstractions](http://blog.getprismatic.com/blog/2012/10/1/prismatics-graph-at-strange-loop.html).

The result is a library that provides the following:

* an easy way to declare dependencies between Cascalog queries
* modularity of Cascalog workflows; they can be merged now, allowing easy reuse of common parts. 
* Easier debugging, because you can focus on certain parts of a workflow without the need to copy code.
* A way to visualize complex flows
* Maybe more, if we can think of new execution strategies e.g. for logging, profiling, debugging etc. [see the Graph blog post](http://blog.getprismatic.com/blog/2012/10/1/prismatics-graph-at-strange-loop.html)

## Constraints

* The name of a function is the name that is used to match dependencies
* Internal dependencies of a graph are treated as intermediate steps which will always output to a temporary seqfile dir. 
* External dependencies are treated as endpoints, and never as intermediate dependencies, thus a requirement for these steps is to define their own output strategy.

## Motivation

Cascalog.checkpoint is great for when you start to build complex flows. It makes your jobs easier to manage, more robust, and it allows you to easily run parts of a job in parallel. Also as a result of all of this, my jobs did often run much faster. So Cascalog.checkpoint is a must for me when writing new jobs. However, when jobs grow more complex or when you have jobs that have workflow parts in common, you will notice that the `workflow` macro makes things hard to reuse and a lot of the code needed feels verbose. This library solves this by generating the workflow code for you based on steps you provide. In the future it might be worthwhile to integrate with Cascalog.checkpoint more directly, but that depends on the success of this library.

## Releases

Cascalog-Graph is on [Clojars](https://clojars.org/adgoji/cascalog-graph)

Latest release is 0.0.1

[Leiningen](https://github.com/technomancy/leiningen) dependency information:

    [adgoji/cascalog-graph "0.0.1"]

The Git master branch is at version 0.0.1-SNAPSHOT.

## Usage

The following example is a copy of [Stuart Sierra's flow example](https://github.com/stuartsierra/flow) molded into Cascalog Graph form.

    (require '[cascalog.api :as cascalog]
             '[adgoji.cascalog.graph :as g])

    (def result (g/flow-fn [gamma delta epsilon output-tap]
      (?<- output-tap 
        [?result]
        (gamma ?idx ?gamma)
        (delta ?idx ?delta)
        (epsilon ?idx ?epsilon)
        (+ ?gamma ?delta ?epsilon :> ?result))))
    
    (def gamma (g/flow-fn [alpha beta]
      (<- [?idx ?gamma]
        (alpha ?idx ?alpha)
        (beta ?idx ?beta)
        (+ ?alpha ?beta :> ?gamma))))
    
    (def delta (g/flow-fn [alpha gamma]
      (<- [?idx ?delta]
        (alpha ?idx ?alpha)
        (gamma ?idx ?gamma)
        (+ ?gamma ?alpha :> ?delta))))
    
    (def epsilon (g/flow-fn [gamma delta]
      (<- [?idx ?epsilon]
        (gamma ?idx ?gamma)
        (delta ?idx ?delta)
        (+ ?gamma ?delta :> ?epsilon))))
    
    (def complete-flow (g/fns-to-flow #'result #'gamma #'delta #'epsilon))
    
    (def complete-flow (g/fns-to-flow #'result #'gamma #'delta #'epsilon))

Print workflow for debugging

    user=> (g/pp-workflow complete-flow)
    gamma-step ([:deps nil :tmp-dirs [gamma-dir]]
      :gamma: 
      (cascalog.api/?- (cascalog.api/hfs-seqfile gamma-dir) (user/gamma {:beta beta, :alpha alpha})))
    delta-step ([:deps [gamma-step] :tmp-dirs [delta-dir]]
      :delta: 
      (cascalog.api/?- (cascalog.api/hfs-seqfile delta-dir) (user/delta {:gamma (cascalog.api/hfs-seqfile gamma-dir), :alpha alpha})))
    epsilon-step ([:deps [delta-step gamma-step] :tmp-dirs [epsilon-dir]]
      :epsilon: 
      (cascalog.api/?- (cascalog.api/hfs-seqfile epsilon-dir) (user/epsilon {:gamma (cascalog.api/hfs-seqfile gamma-dir), :delta (cascalog.api/hfs-seqfile delta-dir)})))
    result-step ([:deps [epsilon-step delta-step gamma-step]]
      :result: 
      (user/result {:gamma (cascalog.api/hfs-seqfile gamma-dir), :delta (cascalog.api/hfs-seqfile delta-dir), :output-tap output-tap, :epsilon (cascalog.api/hfs-seqfile epsilon-dir)}))
    nil


Output dot for visualization:

    user=> (g/dot complete-flow)
    digraph "flow" {
       "alpha" -> "delta" ;
       "alpha" -> "gamma" ;
       "epsilon" -> "result" ;
       "output-tap" -> "result" ;
       "delta" -> "epsilon" ;
       "delta" -> "result" ;
       "beta" -> "gamma" ;
       "gamma" -> "epsilon" ;
       "gamma" -> "result" ;
       "gamma" -> "delta" ;
    }
    nil

Write that out to a file:

    (g/write-dotfile process-flow "flow.dot")

Then run Graphviz:

    $ dot -Tpng -o flow.png flow.dot

Run the workflow

    ((g/mk-workflow-fn complete-flow) {:output-tap (cascalog/stdout) :alpha [[0 1]] :beta [[0 2]] })
    
    ...
    
    RESULTS
    -----------------------
    14

## Todo

* Provide better examples
* Add tests
* Internal code cleanup
* Gather feedback from Cascalog.Checkpoint users

## Credits

* Prismatic for coining the ideas of Graph (see [blogpost]([http://blog.getprismatic.com/blog/2012/10/1/prismatics-graph-at-strange-loop.html))
* Stuart Sierra for building flow [an implementation of the Graph idea](https://github.com/stuartsierra/flow) and an important dependency for this project
* Sam Ritchie and Contributors for building Cascalog.checkpoint that makes this library actually do anything
* [AdGoji](http://www.adgoji.com/) for giving me time to work on this

## License

Copyright © 2013 Jeroen van Dijk

Distributed under the Eclipse Public License, the same as Clojure.
