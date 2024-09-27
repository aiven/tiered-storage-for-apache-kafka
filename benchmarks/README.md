### Benchmarks module

> Borrowed from https://github.com/apache/kafka/blob/trunk/jmh-benchmarks

This module contains benchmarks written using [JMH](https://openjdk.java.net/projects/code-tools/jmh/) from OpenJDK.

### Running benchmarks

If you want to set specific JMH flags or only run certain benchmarks, passing arguments via
gradle tasks is cumbersome. These are simplified by the provided `jmh.sh` script.

The default behavior is to run all benchmarks:

    ./benchmarks/jmh.sh

Pass a pattern or name after the command to select the benchmarks:

    ./benchmarks/jmh.sh TransformBench

Check which benchmarks that match the provided pattern:

    ./benchmarks/jmh.sh -l TransformBench

Run a specific test and override the number of forks, iterations and warm-up iteration to `2`:

    ./benchmarks/jmh.sh -f 2 -i 2 -wi 2 TransformBench

Run a specific test with async and GC profilers on Linux and flame graph output:

    ./benchmarks/jmh.sh -prof gc -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph TransformBench

The following sections cover async profiler and GC profilers in more detail.

### Using JMH with async profiler

It's good practice to check profiler output for micro-benchmarks in order to verify that they represent the expected
application behavior and measure what you expect to measure. Some example pitfalls include the use of expensive mocks
or accidental inclusion of test setup code in the benchmarked code. JMH includes
[async-profiler](https://github.com/jvm-profiling-tools/async-profiler) integration that makes this easy:

    ./benchmarks/jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so

or if having async-profiler on environment variable `export LD_LIBRARY_PATH=/opt/async-profiler-2.9-linux-x64/build/`

    ./benchmarks/jmh.sh -prof async

With flame graph output (the semicolon is escaped to ensure it is not treated as a command separator):

    ./benchmarks/jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph

Simultaneous cpu, allocation and lock profiling with async profiler 2.0 and jfr output (the semicolon is
escaped to ensure it is not treated as a command separator):

    ./benchmarks/jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;output=jfr\;alloc\;lock TransformBench

A number of arguments can be passed to configure async profiler, run the following for a description:

    ./benchmarks/jmh.sh -prof async:help

### Using JMH GC profiler

It's good practice to run your benchmark with `-prof gc` to measure its allocation rate:

    ./benchmarks/jmh.sh -prof gc

Of particular importance is the `norm` alloc rates, which measure the allocations per operation rather than allocations
per second which can increase when you have make your code faster.

### Running JMH outside gradle

The JMH benchmarks can be run outside gradle as you would with any executable jar file:

    java -jar ./benchmarks/build/libs/kafka-benchmarks-*.jar -f2 TransformBench

### Gradle Tasks

If no benchmark mode is specified, the default is used which is throughput. It is assumed that users run
the gradle tasks with `./gradlew` from the root of the Kafka project.

* `benchmarks:shadowJar` - creates the uber jar required to run the benchmarks.

* `benchmarks:jmh` - runs the `clean` and `shadowJar` tasks followed by all the benchmarks.

### JMH Options
Some common JMH options are:

```text

   -e <regexp+>                Benchmarks to exclude from the run. 

   -f <int>                    How many times to fork a single benchmark. Use 0 to 
                               disable forking altogether. Warning: disabling 
                               forking may have detrimental impact on benchmark 
                               and infrastructure reliability, you might want 
                               to use different warmup mode instead.

   -i <int>                    Number of measurement iterations to do. Measurement
                               iterations are counted towards the benchmark score.
                               (default: 1 for SingleShotTime, and 5 for all other
                               modes)

   -l                          List the benchmarks that match a filter, and exit.

   -lprof                      List profilers, and exit.

   -o <filename>               Redirect human-readable output to a given file. 

   -prof <profiler>            Use profilers to collect additional benchmark data. 
                               Some profilers are not available on all JVMs and/or 
                               all OSes. Please see the list of available profilers 
                               with -lprof.

   -v <mode>                   Verbosity mode. Available modes are: [SILENT, NORMAL,
                               EXTRA]

   -wi <int>                   Number of warmup iterations to do. Warmup iterations
                               are not counted towards the benchmark score. (default:
                               0 for SingleShotTime, and 5 for all other modes)
```

To view all options run jmh with the -h flag. 
