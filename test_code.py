from prefect import flow, task
import git
import subprocess
import os
import shutil
import stat
import time
import asyncio


@task
async def clone_or_pull_repo(repo_url, repo_dir):
    if not os.path.exists(repo_dir):
        print(f"Cloning repository into {repo_dir}")
        repo = git.Repo.clone_from(repo_url, repo_dir)
    else:
        print(f"Repository exists. Pulling latest changes in {repo_dir}")
        repo = git.Repo(repo_dir)
        repo.remotes.origin.pull()
    return {"repo_dir": repo_dir}


@task
async def run_script(repo_dir):
    os.chdir(repo_dir)
    output = subprocess.check_output(
        ["python", "prefect_1.py"], universal_newlines=True)
    print(output)
    return output  # Return the output of the script


@task
def cleanup_directory(repo_dir):
    time.sleep(3)  # Or experiment with a longer delay
    try:
        if os.path.exists(repo_dir):
            # Change to parent directory or another safe directory
            os.chdir('..')
            shutil.rmtree(repo_dir, onerror=remove_read_only)
            print(f"Deleted directory: {repo_dir}")
        else:
            print(f"Directory does not exist: {repo_dir}")
    except PermissionError:
        print(
            f"Encountered PermissionError trying to delete {repo_dir}, attempting with cmd")
        subprocess.run(['cmd', '/c', 'rmdir', '/s',
                       '/q', repo_dir], check=True)


def remove_read_only(func, path, exc_info):
    os.chmod(path, stat.S_IWRITE)
    func(path)


@flow(name="Repo Management Flow")
async def repo_management_flow():
    result = await clone_or_pull_repo(
        repo_url="https://github.com/muthugh/important-codes.git",
        repo_dir=os.path.abspath("important-codes")
    )
    repo_dir = result["repo_dir"]
    script_output = await run_script(repo_dir)  # Await the script execution
    cleanup_directory(repo_dir)  # Call without await


# Running the async flow using Prefect's run function
if __name__ == "__main__":
    asyncio.run(repo_management_flow())
