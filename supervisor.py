# -*- coding: utf-8 -*-

"""
 Dead simple supervisor for python scripts to ensure
 script terminated normally and got the job done.
"""

import subprocess
import time
import sys
import os
import timeit

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
    if len(sys.argv[1:]) is 0 or sys.argv[1] in "-h":
        print("usage: supervisor.py [-h] script [args [args ...]]")
        sys.exit(0)
    main([sys.argv[1], *sys.argv[2:]])
