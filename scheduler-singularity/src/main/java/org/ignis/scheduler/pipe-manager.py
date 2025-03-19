import os
import sys
import tempfile
import subprocess
import time

def start_singularity_instances(job_id):
    """Start Singularity instances for the job."""
    container_name = f"{job_id}-container"
    cmd = [
        "singularity", "instance", "start",
        "--cpus", "1",  # Default CPU count
        "--memory", "1024",  # Default memory (1GB)
        "--bind", "/tmp:/tmp",  # Default bind mount
        "ignis-logger",  # Default image
        container_name
    ]
    
    print(f"Starting Singularity instance: {' '.join(cmd)}", file=sys.stderr)
    result = subprocess.run(cmd, capture_output=True)
    
    return {
        "stdout": result.stdout.decode(),
        "stderr": result.stderr.decode(),
        "code": result.returncode
    }

def main(job_id, is_create_job):
    # Create job-specific pipe directory
    pipe_dir = tempfile.mkdtemp(prefix=f"ignis-pipe-{job_id}-")
    pipes = ["in", "out", "err", "code"]
    
    # Create named pipes
    for pipe in pipes:
        os.mkfifo(os.path.join(pipe_dir, pipe), 0o600)
    
    print(f"Created pipes in {pipe_dir}", file=sys.stderr)
    
    # Start Singularity instances if this is a createJob call
    if is_create_job.lower() == "true":
        result = start_singularity_instances(job_id)
        print(f"Instance start result: {result}", file=sys.stderr)
    
    # Main loop for handling commands
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
    if len(sys.argv) != 3:
        print("Usage: pipes_manager.py <job_id> <is_create_job>", file=sys.stderr)
        sys.exit(1)
    
    job_id = sys.argv[1]
    is_create_job = sys.argv[2]
    main(job_id, is_create_job)