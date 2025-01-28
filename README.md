# eacct-view
Terminal plotting tool for energy aware runtime (EAR)

## Usage
```
$ python eacct-view.py -h
usage: eacct-view.py [-h] [-j JobID] [--metrics Metrics [Metrics ...]] [--list-metrics] [--list-architectures]

optional arguments:
  -h, --help            show this help message and exit
  -j JobID, --jobid JobID
                        Plot Roofline and Timeline from eacct tool
  --metrics Metrics [Metrics ...]
                        Metrics to plot timeline
  --list-metrics        List available metrics to plot timeline
  --list-architectures  List the specs for the available archs
  ```

## Simple example
Pass it a jobid and stepid of a job that you ran with the EAR runtime library enabled, i.e. `--ear=on`. It doesn't matter the policy.
```
python eacct-view.py -j 9572777.1
```

And you will get something like this...

<img src="img/eacct_view_example.png"/>