import os
import sys
import tempfile
import subprocess
import time

def main(job_id):
    # Create job-specific pipe directory
    pipe_dir = tempfile.mkdtemp(prefix=f"ignis-pipe")
    pipes = ["in", "out", "err", "code"]
    
    # Create named pipes
    for pipe in pipes:
        os.mkfifo(os.path.join(pipe_dir, pipe), 0o600)
    
    print(f"Created pipes in {pipe_dir}", file=sys.stderr)
    
    while True:
        try:
            with open(os.path.join(pipe_dir, "in"), 'r') as f_in:
                command = f_in.read().strip()
                if not command:
                    time.sleep(0.1)
                    continue
                
                result = subprocess.run(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                with open(os.path.join(pipe_dir, "out"), 'w') as f_out:
                    f_out.write(result.stdout.decode())
                with open(os.path.join(pipe_dir, "err"), 'w') as f_err:
                    f_err.write(result.stderr.decode())
                with open(os.path.join(pipe_dir, "code"), 'w') as f_code:
                    f_code.write(str(result.returncode))
                    
        except Exception as e:
            print(f"Pipe error: {str(e)}", file=sys.stderr)
            time.sleep(1)

if __name__ == "__main__":
    main(sys.argv[1])