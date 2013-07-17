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

Latest release is 0.2.4

[Leiningen](https://github.com/technomancy/leiningen) dependency information:

```clojure
[adgoji/cascalog-graph "0.2.4"]
```

The Git master branch is at version 0.2.5-SNAPSHOT.

## Usage

The following example is a copy of [Stuart Sierra's flow example](https://github.com/stuartsierra/flow) molded into Cascalog Graph form.

```clojure
(require '[cascalog.api :as cascalog]
         '[adgoji.cascalog.graph :as g])

(def result (g/fnk [gamma delta epsilon output-tap]
  (?<- output-tap 
    [?result]
    (gamma ?idx ?gamma)
    (delta ?idx ?delta)
    (epsilon ?idx ?epsilon)
    (+ ?gamma ?delta ?epsilon :> ?result))))

(def gamma (g/query-fnk [alpha-tap beta-tap]
  (<- [?idx ?gamma]
    (alpha-tap ?idx ?alpha)
    (beta-tap ?idx ?beta)
    (+ ?alpha ?beta :> ?gamma))))

(def delta (g/query-fnk [alpha-tap gamma]
  (<- [?idx ?delta]
    (alpha-tap ?idx ?alpha)
    (gamma ?idx ?gamma)
    (+ ?gamma ?alpha :> ?delta))))

(def epsilon (g/query-fnk [gamma delta]
  (<- [?idx ?epsilon]
    (gamma ?idx ?gamma)
    (delta ?idx ?delta)
    (+ ?gamma ?delta :> ?epsilon))))

(def complete-flow {:result result 
                    :gamma gamma 
                    :delta delta 
                    :epsilon epsilon})
```

Create a function that wraps the workflow

```clojure
((g/workflow-compile complete-flow) {:alpha-tap [[0 1]] :beta-tap [[0 2]]}) ;=> 14
```

Or create a command line access point to the workflow

```clojure
(require '[adgoji.cascalog.cli :refer [defjob]])

(defjob example complete-flow)
```
    
And execute it from the command line:

    lein run -m your_namespace.example --alpha-tap alpha.hfs-seqfile --beta-tap beta.hfs-seqfile --output-tap stdout

Output to Graphiz' dot format for visualization:

    lein run -m your_namespace.example --mode dot

Or directly open the Graphiz dot file in preview (tested on a Mac)

    lein run -m your_namespace.example --mode preview

Sometimes you need to validate the input through the command line (e.g. with something like Lemur)

    lein run -m your_namespace.example --mode validation

## Todo

* Test suite
* Expand tap validation
* Better command line validation messages
* Documentation

## Credits

* Prismatic for coining the ideas of Graph (see [blogpost]([http://blog.getprismatic.com/blog/2012/10/1/prismatics-graph-at-strange-loop.html))
* Stuart Sierra for building flow [an implementation of the Graph idea](https://github.com/stuartsierra/flow) 
* Sam Ritchie and Contributors for building Cascalog.checkpoint that makes this library actually do anything
* [AdGoji](http://www.adgoji.com/) for giving me time to work on this

## License

Copyright © 2013 Jeroen van Dijk

Distributed under the Eclipse Public License, the same as Clojure.
