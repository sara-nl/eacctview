from plotter import Plotter
import argparse

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--jobid", metavar="JobID/s", help="Plot Roofline and Timeline from eacct tool", default = [], type=str, nargs='+')
    parser.add_argument("-c", "--csv", metavar="csv", help="Plot Roofline and Timeline from csvfile", type=str, nargs=1)
    parser.add_argument("--xvy-metrics", metavar="METRIC_STRING", help="2D metric v metric plot", default = [], type=str, nargs=2, required=False)
    parser.add_argument("--time-metrics", metavar="METRIC_STRING/S", help="Metrics to plot timeline", default = ["CPI", "MEM_GBS", "GFLOPS"], type=str, nargs='+', required=False)
    parser.add_argument("--list-metrics", action='store_true', help="List available metrics to plot timeline", required=False)
    parser.add_argument("--list-architectures", action='store_true', help="List the specs for the available archs", required=False)
    
    args = parser.parse_args()

    plotter = Plotter()

    if args.list_architectures:
        plotter.print_architecture_specs()
        exit(0)

    if args.list_metrics:
        plotter.print_timeline_metrics()
        exit(0)

    if args.jobid:
        plotter.get_jobid(args.jobid)
        plotter.get_eacct_jobavg()
        plotter.get_eacct_jobloop()

        if args.xvy_metrics:
            plotter.terminal(args.time_metrics, args.xvy_metrics)
        else:
            plotter.terminal(args.time_metrics)

    if args.csv:
        plotter.get_eacct_from_csv(filename=args.csv[0])

        if args.xvy_metrics:
            plotter.terminal(args.time_metrics, args.xvy_metrics)
        else:
            plotter.terminal(args.time_metrics)


if __name__ == '__main__':
    main()
