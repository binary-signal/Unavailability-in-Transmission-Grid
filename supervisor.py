# -*- coding: utf-8 -*-
#
# Dead simple supervisor for python scripts to ensure
# code terminated normally and got the job done.
#
# Usage:
# python supervisor.py python_script_i_want_to_supervise.py args to pass to my script

import subprocess
import time
import sys
import os
from timeit import default_timer as timer

RETRY_INTERVAL = 30  # wait seconds before running again
# after crashing

if __name__ == "__main__":
    if len(sys.argv[1:]) == 0:
        sys.exit("need a python script as input to supervise")

    python_script = sys.argv[1]
    args = sys.argv[2:]

    if (
            os.path.exists(python_script)
            and os.path.isfile(python_script)
            and os.path.split(os.path.abspath(python_script))[-1].endswith(".py")
    ):

        crashes = 0
        return_code = None
        t_start = timer()
        while True:
            try:
                try:
                    print(f"running {python_script} with supervisor")
                    return_code = subprocess.check_call(
                        ["python", python_script, *[arg for arg in args]]
                    )
                except subprocess.CalledProcessError as error:
                    print(
                        f"{python_script} crashed: {error} \n"
                        f"{python_script} will start again in {RETRY_INTERVAL} seconds"
                    )

                    crashes += crashes

                    time.sleep(RETRY_INTERVAL)
                    continue
                else:

                    def human_time(start, end):
                        """
                        return a human readable string for a time interval
                        """

                        hours, rem = divmod(end - start, 3600)
                        minutes, seconds = divmod(rem, 60)
                        return "{:0>2}:{:0>2}:{:0>2}".format(
                            int(hours), int(minutes), int(seconds)
                        )


                    t_end = timer()
                    print(
                        f"supervised process exited normally code: {return_code}\n"
                        f"it crashed {crashes} times until completion\n"
                        f"and it took {human_time(t_start, t_end)} time to complete"
                    )

                    break

            except KeyboardInterrupt:
                print("received SIGINT supervisor is going to quit now")
                break

    else:
        sys.exit(
            f"can't locate {python_script} \n"
            f"or {python_script} is not a file \n"
            f"or {python_script} is not a python script \n"
            f"either way, supervisor is going to quit try better next time"
        )
