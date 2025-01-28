from eacctview.plotter import Plotter
import argparse

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--jobid", metavar="JobID", help="Plot Roofline and Timeline from eacct tool", type=str, nargs=1)
    parser.add_argument("--metrics", metavar="Metrics", help="Metrics to plot timeline", default = ["CPI", "MEM_GBS", "GFLOPS"], type=str, nargs='+', required=False)
    parser.add_argument("--list-metrics", action='store_true', help="List available metrics to plot timeline", required=False)
    parser.add_argument("--list-architectures", action='store_true', help="List the specs for the available archs", required=False)
    
    args = parser.parse_args()

    plotter = Plotter()

    if args.list_architectures:
        plotter.print_architecture_specs()
        exit(1)

    if args.list_metrics:
        plotter.print_timeline_metrics()
        exit(1)

    if args.jobid:
        plotter.get_jobid(args.jobid)
        plotter.get_eacct_jobavg()
        plotter.get_eacct_jobloop()
        plotter.terminal(args.metrics)


if __name__ == '__main__':
    main()
