import subprocess
import time

return_code = None
while True:
    try:
        print("Running main.py with supervisor")
        return_code = subprocess.check_call(['python', 'main.py', '-v', '-r'])
    except subprocess.CalledProcessError as error:
        print(error)
        print(f"main.py crashed with code {return_code}")
        print("sleep for 30 seconds and try again")
        time.sleep(30)
    except KeyboardInterrupt:
        break
    else:
        print(f"supervised process completed  {return_code}")
        break
