Welcome to the Spark documentation!

This readme will walk you through navigating and building the Spark documentation, which is included here with the Spark source code. You can also find documentation specific to release versions of Spark at http://spark.apache.org/documentation.html.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the documentation yourself. Why build it yourself? So that you have the docs that corresponds to whichever version of Spark you currently have checked out of revision control.

## Generating the Documentation HTML

We include the Spark documentation as part of the source (as opposed to using a hosted wiki, such as the github wiki, as the definitive documentation) to enable the documentation to evolve along with the source code and be captured by revision control (currently git). This way the code automatically includes the version of the documentation that is relevant regardless of which version or release you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can read those text files directly if you want. Start with index.md.

The markdown code can be compiled to HTML using the 
[Jekyll tool](http://jekyllrb.com).
To use the `jekyll` command, you will need to have Jekyll installed. 
The easiest way to do this is via a Ruby Gem, see the 
[jekyll installation instructions](http://jekyllrb.com/docs/installation). 
Compiling the site with Jekyll will create a directory called 
_site containing index.md as well as the rest of the compiled files.

You can modify the default Jekyll build as follows:

    # Skip generating API docs (which takes a while)
    $ SKIP_SCALADOC=1 jekyll build
    # Serve content locally on port 4000
    $ jekyll serve --watch
    # Build the site with extra features used on the live page
    $ PRODUCTION=1 jekyll build

## Pygments

We also use pygments (http://pygments.org) for syntax highlighting in documentation markdown pages, so you will also need to install that (it requires Python) by running `sudo easy_install Pygments`.

To mark a block of code in your markdown to be syntax highlighted by jekyll during the compile phase, use the following sytax:

    {% highlight scala %}
    // Your scala code goes here, you can replace scala with many other
    // supported languages too.
    {% endhighlight %}

## API Docs (Scaladoc and Epydoc)

You can build just the Spark scaladoc by running `sbt/sbt doc` from the SPARK_PROJECT_ROOT directory.

Similarly, you can build just the PySpark epydoc by running `epydoc --config epydoc.conf` from the SPARK_PROJECT_ROOT/pyspark directory.

When you run `jekyll` in the docs directory, it will also copy over the scaladoc for the various Spark subprojects into the docs directory (and then also into the _site directory). We use a jekyll plugin to run `sbt/sbt doc` before building the site so if you haven't run it (recently) it may take some time as it generates all of the scaladoc.  The jekyll plugin also generates the PySpark docs using [epydoc](http://epydoc.sourceforge.net/).

NOTE: To skip the step of building and copying over the Scala and Python API docs, run `SKIP_API=1 jekyll`.
