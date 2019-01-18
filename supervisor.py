# -*- coding: utf-8 -*-

"""
 Dead simple supervisor for python scripts to ensure
 script terminated normally and got the job done.


 Why to use this ?

 tl;dr Programmed a web crawler which  crashed many times
 while it was crawling for data, internet went down, got
 banned from servers lots of things can go wrong in the
 wild internet. Needed a way to automatically monitor the
 crawler and restart it when it crashed until it crawled
 all the data I needed.
"""

import subprocess
import time
import sys
import os
import timeit
import argparse

RETRY_INTERVAL = 30  # wait seconds before running again after crashing


def human_time(start, end):
    """
    return a human readable string for time interval
    """

    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{int(hours):0>2}:{int(minutes):0>2}:{seconds:05.2f}"


def main(super_args):
    if not super_args:
        sys.exit("need a python script as input to supervise")

    python_script = super_args[0]
    args = super_args[1:]

    # check input script
    if (
        os.path.exists(python_script)
        and os.path.isfile(python_script)
        and os.path.split(os.path.abspath(python_script))[-1].endswith(".py")
    ):

        crashes = 0
        return_code = None
        t_start = timeit.default_timer()

        while True:

            # try to run python script
            try:
                print(f"running {python_script} with supervisor")
                return_code = subprocess.check_call(
                    ["python", python_script, *[arg for arg in args]]
                )
            except subprocess.CalledProcessError as error:
                # print error message when script fails
                print(
                    f"{python_script} crashed: {error} \n"
                    f"{python_script} will start again in {RETRY_INTERVAL}"
                    f" seconds"
                )

                crashes += crashes

                time.sleep(RETRY_INTERVAL)
                continue
            except KeyboardInterrupt:
                print("received SIGINT supervisor is going to quit now")
                break

            # print some stats about the script
            t_end = timeit.default_timer()
            print(
                f"supervised script exited normally code: {return_code}\n"
                f"crashed {crashes} times until completion "
                f"took {human_time(t_start, t_end)} of time"
            )
            break
    else:
        # error message for invalid input file
        sys.exit(
            f"can't locate input {python_script} \n"
            f"or {python_script} is not a file \n"
            f"or {python_script} is not a python script \n"
            f"either way, supervisor is going to quit try better next time"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Supervisor for python scripts"
    )
    parser.add_argument("script", help="Input python file")
    parser.add_argument(
        "args", help="Args passed to script", nargs="*", default=[]
    )
    args = parser.parse_args()
    main([args.script, *args.args])
