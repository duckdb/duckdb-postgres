#include "process_exec.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"

#ifdef _WIN32
#include <windows.h>
#include <thread>
#else
#include <cerrno>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

static constexpr int PROCESS_TIMEOUT_MS = 30000;

namespace duckdb {

#ifdef _WIN32

static string BuildCommandLine(const vector<string> &argv) {
	string cmd;
	for (size_t i = 0; i < argv.size(); i++) {
		if (i > 0) {
			cmd += ' ';
		}
		const string &arg = argv[i];
		bool needs_quote = arg.empty() || arg.find_first_of(" \t\"") != string::npos;
		if (!needs_quote) {
			cmd += arg;
			continue;
		}
		cmd += '"';
		size_t backslashes = 0;
		for (char c : arg) {
			if (c == '\\') {
				backslashes++;
			} else if (c == '"') {
				cmd.append(backslashes * 2 + 1, '\\');
				cmd += '"';
				backslashes = 0;
			} else {
				cmd.append(backslashes, '\\');
				cmd += c;
				backslashes = 0;
			}
		}
		// Double trailing backslashes before the closing quote
		cmd.append(backslashes * 2, '\\');
		cmd += '"';
	}
	return cmd;
}

static void DrainPipe(HANDLE handle, string &out) {
	char buf[4096];
	DWORD n;
	while (ReadFile(handle, buf, sizeof(buf), &n, NULL) && n > 0) {
		out.append(buf, n);
	}
}

ProcessResult RunProcess(const vector<string> &argv) {
	if (argv.empty()) {
		throw IOException("RunProcess: empty argv");
	}

	SECURITY_ATTRIBUTES sa = {};
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;

	HANDLE stdout_r, stdout_w, stderr_r, stderr_w;
	if (!CreatePipe(&stdout_r, &stdout_w, &sa, 0) || !CreatePipe(&stderr_r, &stderr_w, &sa, 0)) {
		throw IOException("RunProcess: CreatePipe failed");
	}
	// Don't let the child inherit the read ends
	SetHandleInformation(stdout_r, HANDLE_FLAG_INHERIT, 0);
	SetHandleInformation(stderr_r, HANDLE_FLAG_INHERIT, 0);

	STARTUPINFOA si = {};
	si.cb = sizeof(si);
	si.dwFlags = STARTF_USESTDHANDLES;
	si.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
	si.hStdOutput = stdout_w;
	si.hStdError = stderr_w;

	PROCESS_INFORMATION pi = {};
	string cmd = BuildCommandLine(argv);

	if (!CreateProcessA(NULL, &cmd[0], NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi)) {
		CloseHandle(stdout_r);
		CloseHandle(stdout_w);
		CloseHandle(stderr_r);
		CloseHandle(stderr_w);
		DWORD err = GetLastError();
		if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) {
			throw IOException("AWS CLI not found on PATH. Install from "
			                  "https://aws.amazon.com/cli/ and ensure 'aws' is on PATH.");
		}
		throw IOException("RunProcess: CreateProcess('%s') failed (error=%lu)", argv[0].c_str(), err);
	}
	CloseHandle(stdout_w);
	CloseHandle(stderr_w);
	CloseHandle(pi.hThread);

	ProcessResult result;
	string stdout_buf, stderr_buf;
	std::thread t_out(DrainPipe, stdout_r, std::ref(stdout_buf));
	std::thread t_err(DrainPipe, stderr_r, std::ref(stderr_buf));

	if (WaitForSingleObject(pi.hProcess, PROCESS_TIMEOUT_MS) == WAIT_TIMEOUT) {
		TerminateProcess(pi.hProcess, 1);
		WaitForSingleObject(pi.hProcess, INFINITE);
		t_out.join();
		t_err.join();
		CloseHandle(stdout_r);
		CloseHandle(stderr_r);
		CloseHandle(pi.hProcess);
		throw IOException("RunProcess: '%s' timed out after 30 seconds", argv[0].c_str());
	}

	t_out.join();
	t_err.join();

	DWORD exit_code = 0;
	GetExitCodeProcess(pi.hProcess, &exit_code);
	CloseHandle(stdout_r);
	CloseHandle(stderr_r);
	CloseHandle(pi.hProcess);

	result.exit_code = static_cast<int>(exit_code);
	result.stdout_str = std::move(stdout_buf);
	result.stderr_str = std::move(stderr_buf);
	return result;
}

#else // POSIX

ProcessResult RunProcess(const vector<string> &argv) {
	if (argv.empty()) {
		throw IOException("RunProcess: empty argv");
	}

	vector<const char *> c_args;
	c_args.reserve(argv.size() + 1);
	for (const auto &s : argv) {
		c_args.push_back(s.c_str());
	}
	c_args.push_back(nullptr);

	int stdout_pipe[2], stderr_pipe[2], err_pipe[2];
	if (pipe(stdout_pipe) != 0 || pipe(stderr_pipe) != 0 || pipe(err_pipe) != 0) {
		throw IOException("RunProcess: pipe() failed: %s", strerror(errno));
	}

	pid_t pid = fork();
	if (pid < 0) {
		close(stdout_pipe[0]);
		close(stdout_pipe[1]);
		close(stderr_pipe[0]);
		close(stderr_pipe[1]);
		close(err_pipe[0]);
		close(err_pipe[1]);
		throw IOException("RunProcess: fork() failed: %s", strerror(errno));
	}

	if (pid == 0) {
		dup2(stdout_pipe[1], STDOUT_FILENO);
		dup2(stderr_pipe[1], STDERR_FILENO);
		close(stdout_pipe[0]);
		close(stdout_pipe[1]);
		close(stderr_pipe[0]);
		close(stderr_pipe[1]);
		close(err_pipe[0]);
		// Close-on-exec: if execvp succeeds this fd closes automatically (parent reads 0 bytes)
		fcntl(err_pipe[1], F_SETFD, FD_CLOEXEC);
		execvp(c_args[0], const_cast<char *const *>(c_args.data()));
		// execvp failed — write errno to err_pipe so parent can report it
		int exec_errno = errno;
		(void)write(err_pipe[1], &exec_errno, sizeof(exec_errno));
		_exit(127);
	}

	// Parent: close the write ends we no longer need
	close(stdout_pipe[1]);
	close(stderr_pipe[1]);
	close(err_pipe[1]);

	// Check if exec failed (err_pipe returns data) or succeeded (returns 0 bytes, close-on-exec fired)
	int exec_errno = 0;
	ssize_t n = read(err_pipe[0], &exec_errno, sizeof(exec_errno));
	close(err_pipe[0]);
	if (n == static_cast<ssize_t>(sizeof(exec_errno))) {
		waitpid(pid, nullptr, 0);
		close(stdout_pipe[0]);
		close(stderr_pipe[0]);
		if (exec_errno == ENOENT) {
			throw IOException("AWS CLI not found on PATH. Install from "
			                  "https://aws.amazon.com/cli/ and ensure 'aws' is on PATH.");
		}
		throw IOException("RunProcess: cannot exec '%s': %s", argv[0].c_str(), strerror(exec_errno));
	}

	// Poll both pipes until closed or timeout (30s)
	ProcessResult result;
	int pipe_fds[2] = {stdout_pipe[0], stderr_pipe[0]};
	string *targets[2] = {&result.stdout_str, &result.stderr_str};
	bool pipe_closed[2] = {false, false};

	auto deadline = steady_clock::now() + std::chrono::milliseconds(PROCESS_TIMEOUT_MS);

	char buf[4096];
	while (!pipe_closed[0] || !pipe_closed[1]) {
		long remaining_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(deadline - steady_clock::now()).count();
		if (remaining_ms <= 0) {
			kill(pid, SIGKILL);
			waitpid(pid, nullptr, 0);
			close(stdout_pipe[0]);
			close(stderr_pipe[0]);
			throw IOException("RunProcess: '%s' timed out after 30 seconds", argv[0].c_str());
		}

		struct pollfd fds[2] = {{pipe_closed[0] ? -1 : pipe_fds[0], POLLIN, 0},
		                        {pipe_closed[1] ? -1 : pipe_fds[1], POLLIN, 0}};

		int ret = poll(fds, 2, static_cast<int>(remaining_ms));
		if (ret < 0) {
			if (errno == EINTR) {
				continue;
			}
			break;
		}

		for (int i = 0; i < 2; i++) {
			if (pipe_closed[i]) {
				continue;
			}
			if (fds[i].revents & POLLIN) {
				ssize_t nr = read(pipe_fds[i], buf, sizeof(buf));
				if (nr > 0) {
					targets[i]->append(buf, static_cast<size_t>(nr));
				} else {
					pipe_closed[i] = true;
				}
			}
			if ((fds[i].revents & (POLLHUP | POLLERR)) && !pipe_closed[i]) {
				// Drain any remaining bytes then mark closed
				ssize_t nr;
				while ((nr = read(pipe_fds[i], buf, sizeof(buf))) > 0) {
					targets[i]->append(buf, static_cast<size_t>(nr));
				}
				pipe_closed[i] = true;
			}
		}
	}

	close(stdout_pipe[0]);
	close(stderr_pipe[0]);

	int status = 0;
	waitpid(pid, &status, 0);
	result.exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
	return result;
}

#endif // _WIN32

} // namespace duckdb
