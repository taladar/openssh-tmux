#![deny(unknown_lints)]
#![deny(renamed_and_removed_lints)]
#![forbid(unsafe_code)]
#![deny(deprecated)]
#![forbid(private_in_public)]
#![forbid(non_fmt_panics)]
#![deny(unreachable_code)]
#![deny(unreachable_patterns)]
#![forbid(unused_doc_comments)]
#![forbid(unused_must_use)]
#![deny(while_true)]
#![deny(unused_parens)]
#![deny(redundant_semicolons)]
#![deny(non_ascii_idents)]
#![deny(confusable_idents)]
#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::cargo_common_metadata)]
#![warn(rustdoc::missing_crate_level_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_debug_implementations)]
#![deny(clippy::mod_module_files)]
//#![warn(clippy::pedantic)]
#![warn(clippy::redundant_else)]
#![warn(clippy::must_use_candidate)]
#![warn(clippy::missing_panics_doc)]
#![warn(clippy::missing_errors_doc)]
#![warn(clippy::panic)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
#![doc = include_str!("../README.md")]

use std::{
    collections::BTreeMap,
    os::unix::prelude::ExitStatusExt,
    process::ExitStatus,
    str::{from_utf8, Utf8Error},
};

use postage::prelude::{Sink, Stream};
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

/// the error type for the library
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// error in the openssh crate
    #[error("openssh Error: {0}")]
    OpenSSHError(#[from] openssh::Error),
    /// error decoding Utf-8
    #[error("error when decoding Utf-8: {0}")]
    Utf8DecodeError(#[from] Utf8Error),
    /// error joining tokio task
    #[error("error joining tokio task: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    /// failure when running a remote tmux command
    #[error("tmux command {0} failed with exit code {1} and stdout {2} and stderr {3}")]
    TmuxCommandFailed(String, std::process::ExitStatus, String, String),
    /// error parsing an exit status value from a string
    #[error("error parsing exit status: {0}")]
    ParseExitStatusError(std::num::ParseIntError),
    /// error sending data through the channel from the task watching the fifo for tmux messages to the future waiting for the command result
    #[error("error sending through command result channel: {0}")]
    ChannelSendError(Box<postage::sink::SendError<CommandReturn>>),
    /// error sending data through the channel from the close function to the task watching the fifo
    #[error("error sending through quit channel: {0}")]
    QuitChannelSendError(Box<postage::sink::SendError<()>>),
    /// channel was closed when we were polling it before we got a value
    #[error("channel was closed")]
    ChannelClosed,
    /// unexpected result splitting fifo message
    #[error("unexpected result splitting fifo message: {0:?}")]
    UnexpectedSplitResult(Vec<String>),
    /// error reading from fifo
    #[error("fifo read error: {0}")]
    FifoReadError(std::io::Error),
    /// error in fifo reader task means all remaining channels get this error as a message
    #[error("fifo reading aborted")]
    FifoReadingAborted,
    /// end of loop in fifo reader task means all remaining channels get this error as a message
    #[error("fifo reading ended")]
    FifoReadingEnded,
    /// error parsing tmux pane id
    #[error("error parsing tmux pane id: {0}")]
    ParsePaneId(std::num::ParseIntError),
    /// unexpected character in tmux session name
    #[error("unexpected character in tmux session name: {0}")]
    UnexpectedCharacterInTmuxSessionName(String),
    /// unexpected character in tmux window name
    #[error("unexpected character in tmux window name: {0}")]
    UnexpectedCharacterInTmuxWindowName(String),
    /// unexpected split result (e.g. three elements) when splitting on colon in window name
    #[error("unexpected split result when parsing tmux window: {0:?}")]
    UnexpectedSplitResultWhenParsingTmuxWindow(Vec<String>),
    /// unexpected split result (e.g. three elements) when splitting on dot in window name
    #[error("unexpected split result when parsing tmux pane: {0:?}")]
    UnexpectedSplitResultWhenParsingTmuxPane(Vec<String>),
    /// error parsing command id as uuid
    #[error("command uuid parse error: {0}")]
    CommandUuidParseError(uuid::Error),
}

/// run a command in an existing [openssh::Session] and return exit status,
/// stdout and stderr
///
/// # Errors
///
/// this fails if running the command fails for some SSH related reason but
/// not if it returns an exit code that is not success
pub async fn run_openssh_command<'a, S1, I, S2>(
    session: &openssh::Session,
    program: S1,
    args: I,
) -> Result<(std::process::ExitStatus, String, String), Error>
where
    S1: Into<std::borrow::Cow<'a, str>> + std::fmt::Debug,
    I: IntoIterator<Item = S2> + std::fmt::Debug,
    S2: AsRef<str> + std::fmt::Debug,
{
    let mut command = session.command(program);
    command.args(args);
    tracing::trace!("Running command: {:#?}", command);
    let output = command.output().await?;
    tracing::trace!("Exit status: {}", output.status);
    let stdout = from_utf8(&output.stdout)?.to_string();
    tracing::trace!("stdout:\n{}", &stdout);
    let stderr = from_utf8(&output.stderr)?.to_string();
    tracing::trace!("stderr:\n{}", &stderr);
    Ok((output.status, stdout, stderr))
}

/// newtype for tmux socket path
#[derive(Debug, Clone, Hash, PartialEq, Eq, derive_more::Display, derive_more::FromStr)]
pub struct TmuxSocket(String);

/// newtype for tmux session name
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, derive_more::Display)]
pub struct TmuxSessionName(String);

impl std::str::FromStr for TmuxSessionName {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Ok(TmuxSessionName(s.to_string()))
        } else {
            Err(crate::Error::UnexpectedCharacterInTmuxSessionName(
                s.to_string(),
            ))
        }
    }
}

/// newtype for tmux window name
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, derive_more::Display)]
pub struct TmuxWindowName(String);

impl std::str::FromStr for TmuxWindowName {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Ok(TmuxWindowName(s.to_string()))
        } else {
            Err(crate::Error::UnexpectedCharacterInTmuxWindowName(
                s.to_string(),
            ))
        }
    }
}

/// newtype for tmux pane id
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, derive_more::Display)]
pub struct TmuxPaneId(u64);

impl std::str::FromStr for TmuxPaneId {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<u64>() {
            Ok(pane_id) => Ok(TmuxPaneId(pane_id)),
            Err(e) => Err(crate::Error::ParsePaneId(e)),
        }
    }
}

/// tmux session
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TmuxSession {
    /// the name of the session
    session_name: TmuxSessionName,
}

impl std::fmt::Display for TmuxSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.session_name)
    }
}

impl std::str::FromStr for TmuxSession {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let session_name: TmuxSessionName = s.parse()?;
        Ok(TmuxSession { session_name })
    }
}

/// tmux window
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TmuxWindow {
    /// the session for the window
    session: TmuxSession,
    /// the name of the window
    window_name: TmuxWindowName,
}

impl TmuxWindow {
    /// turns the [TmuxWindow] into a [TmuxPane] with a pane id 0
    fn default_pane(&self) -> TmuxPane {
        TmuxPane {
            window: self.clone(),
            pane_id: TmuxPaneId(0),
        }
    }
}

impl std::fmt::Display for TmuxWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.session, self.window_name)
    }
}

impl std::str::FromStr for TmuxWindow {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split = s.split(':').collect::<Vec<&str>>();
        match split.as_slice() {
            [s, w] => {
                let session: TmuxSession = s.parse()?;
                let window_name: TmuxWindowName = w.parse()?;
                Ok(TmuxWindow {
                    session,
                    window_name,
                })
            }
            _ => {
                return Err(crate::Error::UnexpectedSplitResultWhenParsingTmuxWindow(
                    split.iter().map(|s| s.to_string()).collect(),
                ));
            }
        }
    }
}

/// tmux pane
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TmuxPane {
    /// the window for the pane
    window: TmuxWindow,
    /// the id of the pane
    pane_id: TmuxPaneId,
}

impl std::fmt::Display for TmuxPane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.window, self.pane_id)
    }
}

impl std::str::FromStr for TmuxPane {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split = s.split('.').collect::<Vec<&str>>();
        match split.as_slice() {
            [w, p] => {
                let window: TmuxWindow = w.parse()?;
                let pane_id: TmuxPaneId = p.parse()?;
                Ok(TmuxPane { window, pane_id })
            }
            _ => {
                return Err(crate::Error::UnexpectedSplitResultWhenParsingTmuxPane(
                    split.iter().map(|s| s.to_string()).collect(),
                ));
            }
        }
    }
}

/// store all information relevant to a remote Tmux session accessed via SSH
#[derive(Debug)]
pub struct Tmux<'a> {
    /// the tmux socket
    tmux_socket: TmuxSocket,
    /// the openssh session
    openssh_session: &'a openssh::Session,
}

/// tmux option scope
#[derive(Debug)]
pub enum TmuxOptionScope {
    /// server-wide option
    Server,
    /// global session option
    GlobalSession,
    /// session option
    Session(TmuxSession),
    /// global window option
    GlobalWindow,
    /// window option
    Window(TmuxWindow),
    /// pane option
    Pane(TmuxPane),
}

impl<'a> Tmux<'a> {
    /// create a new Tmux object
    #[must_use]
    pub fn new(tmux_socket: TmuxSocket, openssh_session: &'a openssh::Session) -> Self {
        Tmux {
            tmux_socket,
            openssh_session,
        }
    }

    /// run a tmux command remotely with the correct socket set
    async fn run_tmux_command<'b, I, S>(
        &'a self,
        tmux_command: &'b str,
        arguments: I,
    ) -> Result<(std::process::ExitStatus, String, String), Error>
    where
        I: IntoIterator<Item = S> + std::fmt::Debug,
        S: AsRef<str> + std::fmt::Debug + 'b,
        Vec<&'b str>: Extend<S>,
        'a: 'b,
    {
        let mut args = vec!["-S", &self.tmux_socket.0];
        args.push(tmux_command.as_ref());
        args.extend(arguments.into_iter().collect::<Vec<_>>());
        run_openssh_command(self.openssh_session, "tmux", args).await
    }

    /// return all the tmux sessions on the remote tmux server
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn list_sessions(&self) -> Result<Vec<TmuxSession>, Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command("list-sessions", &["-F", "#S"])
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "list-sessions".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        let tmux_sessions = stdout
            .lines()
            .map(|s| TmuxSession {
                session_name: TmuxSessionName(s.to_string()),
            })
            .collect::<Vec<TmuxSession>>();
        tracing::trace!("tmux sessions on server:\n{:#?}", tmux_sessions);
        Ok(tmux_sessions)
    }

    /// return all tmux windows in the given session
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    #[allow(dead_code)]
    pub async fn list_windows(&self, session: &TmuxSession) -> Result<Vec<TmuxWindow>, Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command(
                "list-windows",
                &["-t", &format!("={}", session), "-F", "#{window_id}"],
            )
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "list-windows".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        let tmux_windows = stdout
            .lines()
            .map(|s| TmuxWindow {
                session: session.to_owned(),
                window_name: TmuxWindowName(s.to_string()),
            })
            .collect::<Vec<TmuxWindow>>();
        tracing::trace!(
            "tmux windows in session {} on server:\n{:#?}",
            session,
            tmux_windows
        );
        Ok(tmux_windows)
    }

    /// return all tmux panes in the given window
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    #[allow(dead_code)]
    pub async fn list_panes(&self, window: &TmuxWindow) -> Result<Vec<TmuxPane>, Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command("list-panes", &["-t", &format!("={}", window), "-F", "#D"])
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "list-panes".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        let tmux_panes = stdout
            .lines()
            .map(|s| {
                Ok(TmuxPane {
                    window: window.to_owned(),
                    pane_id: TmuxPaneId(s.to_string().parse().map_err(crate::Error::ParsePaneId)?),
                })
            })
            .collect::<Result<Vec<TmuxPane>, Error>>()?;
        tracing::trace!(
            "tmux panes in window {} on server:\n{:#?}",
            window,
            tmux_panes
        );
        Ok(tmux_panes)
    }

    /// create a new tmux session in the background
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn new_session(&self, session_name: &TmuxSessionName) -> Result<TmuxSession, Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command(
                "new-session",
                &["-d", "-s", &session_name.to_string(), "-P", "-F", "#S"],
            )
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "new-session".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        tracing::trace!("created tmux session {}", &stdout);
        Ok(TmuxSession {
            session_name: TmuxSessionName(stdout.trim().to_string()),
        })
    }

    /// create a new tmux window and return an identifier for it
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn new_window(&self, session: &TmuxSession) -> Result<TmuxWindow, Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command(
                "new-window",
                &["-t", &format!("={}", session), "-P", "-F", "#{window_id}"],
            )
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "new-window".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        let window = TmuxWindow {
            session: session.to_owned(),
            window_name: TmuxWindowName(stdout.trim().to_owned()),
        };
        tracing::trace!("created tmux window {}", &window);
        Ok(window)
    }

    /// returns the scope arguments needed for set-option to set options in the scope described by the parameter
    #[must_use]
    pub fn option_scope_arguments(option_scope: TmuxOptionScope) -> Vec<String> {
        match option_scope {
            TmuxOptionScope::Server => vec!["-s".to_string()],
            TmuxOptionScope::GlobalSession => vec!["-g".to_string()],
            TmuxOptionScope::Session(session) => vec!["-t".to_string(), format!("={}", session)],
            TmuxOptionScope::GlobalWindow => vec!["-g".to_string(), "-w".to_string()],
            TmuxOptionScope::Window(window) => {
                vec!["-w".to_string(), "-t".to_string(), format!("={}", window)]
            }
            TmuxOptionScope::Pane(pane) => {
                vec!["-p".to_string(), "-t".to_string(), format!("={}", pane)]
            }
        }
    }

    /// set tmux option
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn set_option(
        &self,
        option_scope: TmuxOptionScope,
        option_name: &str,
        option_value: &str,
    ) -> Result<(), Error> {
        let mut args: Vec<&str> = Vec::new();
        let option_args = Self::option_scope_arguments(option_scope);
        args.extend(
            option_args
                .iter()
                .map(std::ops::Deref::deref)
                .collect::<Vec<_>>(),
        );
        args.extend([option_name, option_value]);
        let (status, stdout, stderr) = self.run_tmux_command("set-option", &args).await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "set-option".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        tracing::trace!(
            "set tmux option {} to value {}",
            &option_name,
            &option_value
        );
        Ok(())
    }

    /// set tmux hook
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn set_hook(
        &self,
        option_scope: TmuxOptionScope,
        hook_name: &str,
        hook_command: &str,
    ) -> Result<(), Error> {
        let mut args: Vec<&str> = Vec::new();
        let option_args = Self::option_scope_arguments(option_scope);
        args.extend(
            option_args
                .iter()
                .map(std::ops::Deref::deref)
                .collect::<Vec<_>>(),
        );
        args.extend([hook_name, hook_command]);
        let (status, stdout, stderr) = self.run_tmux_command("set-hook", &args).await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "set-hook".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        tracing::trace!("set tmux hook {} to command {}", &hook_name, &hook_command);
        Ok(())
    }

    /// pipe tmux pane
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn pipe_pane_output_to(
        &self,
        pane: &TmuxPane,
        pipe_command: &str,
    ) -> Result<(), Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command(
                "pipe-pane",
                &["-O", "-t", &format!("={}", pane), pipe_command],
            )
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "pipe-pane".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        tracing::trace!("piping output of pane {} to {}", &pane, &pipe_command);
        Ok(())
    }

    /// respawn tmux pane
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn respawn_pane(&self, pane: &TmuxPane, command: &str) -> Result<(), Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command(
                "respawn-pane",
                &["-k", "-t", &format!("={}", pane), command],
            )
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "respawn-pane".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        tracing::trace!("respawning pane {} with new command {}", &pane, &command);
        Ok(())
    }

    /// kill tmux pan
    ///
    /// # Errors
    ///
    /// this fails if either something goes wrong on the SSH transport
    /// or tmux returns an error exit code
    pub async fn kill_pane(&self, pane: &TmuxPane) -> Result<(), Error> {
        let (status, stdout, stderr) = self
            .run_tmux_command("kill-pane", &["-t", &format!("={}", pane)])
            .await?;
        if !status.success() {
            return Err(Error::TmuxCommandFailed(
                "kill-pane".to_string(),
                status,
                stdout,
                stderr,
            ));
        }
        tracing::trace!("killing pane {}", &pane);
        Ok(())
    }
}

/// what a command returns
pub type CommandReturn = Result<(std::process::ExitStatus, String), Error>;

/// the future returned when invoking a command in a remote tmux that will yield the exit status and output
#[derive(Debug)]
#[pin_project::pin_project]
pub struct CommandResult {
    /// the receiver end of a channel that will yield the exit status and output
    #[pin]
    channel_receiver: postage::oneshot::Receiver<CommandReturn>,
}

impl futures::Future for CommandResult {
    type Output = CommandReturn;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match this
            .channel_receiver
            .poll_recv(&mut postage::Context::from_waker(cx.waker()))
        {
            postage::stream::PollRecv::Ready(v) => std::task::Poll::Ready(v),
            postage::stream::PollRecv::Pending => std::task::Poll::Pending,
            postage::stream::PollRecv::Closed => std::task::Poll::Ready(Err(Error::ChannelClosed)),
        }
    }
}

/// runs commands in the background via tmux on a server
#[derive(Debug)]
pub struct TmuxCommandRunner {
    /// the host and optionally user and port in a format acceptable to the openssh crate
    ssh_destination: String,
    /// the temporary directory on the remote host where the tmux socket and our temporary files
    /// for command output,... are stored
    tmux_tmp_dir: String,
    /// the tmux socket name
    tmux_socket: TmuxSocket,
    /// the tmux socket filename, also used as basename for some of the state files
    tmux_socket_filename: String,
    /// the tmux session name
    tmux_session: TmuxSession,
    /// map from command id to channel where we send the result
    command_to_channel: std::sync::Arc<
        tokio::sync::Mutex<BTreeMap<uuid::Uuid, postage::oneshot::Sender<CommandReturn>>>,
    >,
    /// the join handle for the background task which listens to the fifo on the server side
    /// that tmux hooks write into for completed commands (on the pane_died hook)
    join_handle: tokio::task::JoinHandle<Result<(), Error>>,
    /// the channel to signal the background task to stop listening on the fifo
    quit_channel_sender: postage::oneshot::Sender<()>,
}

impl TmuxCommandRunner {
    /// create a new command runner and set up necessary tmux session and options on the server side
    ///
    /// # Errors
    ///
    /// this fails if we can not connect to the server and create the tmux session with the required hooks and options
    pub async fn new(
        ssh_destination: String,
        tmux_tmp_dir: String,
        tmux_socket_filename: String,
        tmux_session_name: TmuxSessionName,
    ) -> Result<Self, Error> {
        let tmux_socket = TmuxSocket(format!("{}/tmux-0/{}", tmux_tmp_dir, tmux_socket_filename));
        let fifo_name = format!("{}/{}.fifo", tmux_tmp_dir, tmux_socket_filename);

        // initialize tmux for our purposes
        let session =
            openssh::Session::connect(&ssh_destination, openssh::KnownHosts::Strict).await?;

        let tmux = Tmux::new(tmux_socket.to_owned(), &session);

        let tmux_sessions = match tmux.list_sessions().await {
            Ok(sessions) => sessions,
            Err(Error::TmuxCommandFailed(_, _, _, stderr))
                if stderr.starts_with("no server running") =>
            {
                vec![]
            }
            Err(Error::TmuxCommandFailed(_, _, _, stderr))
                if stderr.starts_with(&format!(
                    "error connecting to {} (No such file or directory)",
                    tmux_socket
                )) =>
            {
                vec![]
            }
            Err(e) => return Err(e),
        };

        let tmux_session = if !tmux_sessions.contains(&TmuxSession {
            session_name: tmux_session_name.clone(),
        }) {
            // it is possible that new_session returns a session name that differs from the one we passed in
            tmux.new_session(&tmux_session_name).await?
        } else {
            TmuxSession {
                session_name: tmux_session_name,
            }
        };

        tmux.set_option(TmuxOptionScope::GlobalSession, "remain-on-exit", "on")
            .await?;
        tmux.set_option(TmuxOptionScope::GlobalSession, "monitor-activity", "on")
            .await?;
        tmux.set_option(TmuxOptionScope::GlobalSession, "monitor-silence", "1")
            .await?;
        tmux.set_option(TmuxOptionScope::GlobalSession, "activity-action", "any")
            .await?;
        tmux.set_option(TmuxOptionScope::GlobalSession, "silence-action", "any")
            .await?;

        tmux.set_hook(
            TmuxOptionScope::GlobalSession,
            "alert-activity",
            &format!(
                r##"run-shell -b "echo 1 > {}/{}_#S:#I.#P.active""##,
                tmux_tmp_dir, tmux_socket_filename,
            ),
        )
        .await?;

        tmux.set_hook(
            TmuxOptionScope::GlobalSession,
            "alert-silence",
            &format!(
                r##"run-shell -b "echo 0 > {}/{}_#S:#I.#P.active""##,
                tmux_tmp_dir, tmux_socket_filename,
            ),
        )
        .await?;

        tmux.set_hook(
            TmuxOptionScope::GlobalSession,
            "after-kill-pane",
            &format!(
                r##"run-shell -b "rm {}/{}_#S:#I.#P.active""##,
                tmux_tmp_dir, tmux_socket_filename,
            ),
        )
        .await?;

        let (status, _, _) = run_openssh_command(&session, "test", &["-e", &fifo_name]).await?;

        if !status.success() {
            run_openssh_command(&session, "mkfifo", &[&fifo_name]).await?;
        }

        tmux.set_hook(
            TmuxOptionScope::GlobalSession,
            "pane-died",
            &format!(
                r##"run-shell -b "echo 'pane-died|#S:#I.#P|#{{pane_dead_signal}}|#{{pane_dead_status}}|#{{pane_dead_time}}|#{{@pane_command_id}}|#{{pane_start_command}}' > {}""##,
                fifo_name,
            ),
        ).await?;

        session.close().await?;

        // tmux command result handling
        let command_to_channel: std::sync::Arc<
            tokio::sync::Mutex<BTreeMap<uuid::Uuid, postage::oneshot::Sender<CommandReturn>>>,
        > = std::sync::Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (quit_channel_sender, mut quit_channel_receiver) = postage::oneshot::channel();
        let command_to_channel_task = std::sync::Arc::clone(&command_to_channel);
        let tmux_tmp_dir_task = tmux_tmp_dir.to_owned();
        let tmux_socket_task = tmux_socket.to_owned();
        let ssh_destination_task = ssh_destination.to_owned();
        let tmux_socket_filename_task = tmux_socket_filename.to_owned();
        let remote_child_join_handle = tokio::spawn(async move {
            let tmux_socket = tmux_socket_task;
            let tmux_tmp_dir = tmux_tmp_dir_task;
            let command_to_channel = command_to_channel_task;
            let ssh_destination = ssh_destination_task;
            let tmux_socket_filename = tmux_socket_filename_task;
            let session =
                openssh::Session::connect(ssh_destination, openssh::KnownHosts::Strict).await?;
            let tmux = Tmux::new(tmux_socket, &session);
            let mut command = session.command("tail");
            command.arg("-f");
            command.arg(&fifo_name);
            command.stdout(openssh::process::Stdio::piped());
            tracing::trace!("Running command: {:#?}", command);
            let mut remote_child = command.spawn().await?;
            if let Some(stdout) = remote_child.stdout() {
                let buf_reader = BufReader::new(stdout);
                let mut line_reader = LinesStream::new(buf_reader.lines());
                tracing::debug!("Started listening on FIFO");
                while let Some(l) = tokio::select! {
                    l = line_reader.next() => {
                        if l.is_none() {
                            tracing::debug!("Shut down command runner background task because fifo reader returned None");
                        }
                        l
                    }
                    _ = quit_channel_receiver.recv() => {
                        tracing::debug!("Explicitly shut down command runner background task via quit channel");
                        None
                    }
                } {
                    match l {
                        Ok(l) => {
                            tracing::debug!("Read line from fifo:\n{}", l);
                            match &l.split('|').collect::<Vec<&str>>()[..] {
                                ["pane-died", pane, _signal, status, _time, command_id, start_command @ ..] =>
                                {
                                    let start_command = start_command.join("|");
                                    let status: i32 =
                                        status.parse().map_err(Error::ParseExitStatusError)?;
                                    let status = ExitStatus::from_raw(status);
                                    let pane: TmuxPane = pane.parse()?;
                                    let command_id = uuid::Uuid::parse_str(command_id)
                                        .map_err(crate::Error::CommandUuidParseError)?;
                                    tracing::debug!(
                                        "Pane {} running command {} died with exit status {}",
                                        pane,
                                        start_command,
                                        status
                                    );
                                    let (_, output, _) = run_openssh_command(
                                        &session,
                                        "cat",
                                        &[format!(
                                            "{}/{}_{}.log",
                                            &tmux_tmp_dir, tmux_socket_filename, pane
                                        )],
                                    )
                                    .await?;

                                    {
                                        let mut command_to_channel =
                                            command_to_channel.lock().await;

                                        match (*command_to_channel).remove(&command_id) {
                                            Some(mut sender) => {
                                                sender.send(Ok((status, output))).await.map_err(
                                                    |e| Error::ChannelSendError(Box::new(e)),
                                                )?;
                                            }
                                            None => {
                                                tracing::error!("Got command results from pipe but there was no channel to send it to: {} {}", command_id, start_command);
                                                tracing::error!(
                                                    "Channels in command_to_channel map:\n{:#?}",
                                                    (*command_to_channel).keys()
                                                );
                                            }
                                        }
                                    }
                                    run_openssh_command(
                                        &session,
                                        "rm",
                                        &[format!(
                                            "{}/{}_{}.log",
                                            &tmux_tmp_dir, tmux_socket_filename, pane
                                        )],
                                    )
                                    .await?;
                                    run_openssh_command(
                                        &session,
                                        "rm",
                                        &[format!(
                                            "{}/{}_{}.active",
                                            &tmux_tmp_dir, tmux_socket_filename, pane
                                        )],
                                    )
                                    .await?;

                                    tmux.kill_pane(&pane).await?;
                                }
                                sl => {
                                    tracing::error!("Unexpected split result:\n{:#?}", sl);
                                    {
                                        let mut command_to_channel =
                                            command_to_channel.lock().await;

                                        for sender in (*command_to_channel).values_mut() {
                                            sender
                                                .send(Err(Error::FifoReadingAborted))
                                                .await
                                                .map_err(|e| {
                                                    Error::ChannelSendError(Box::new(e))
                                                })?;
                                        }
                                        (*command_to_channel).clear();
                                    }

                                    return Err(Error::UnexpectedSplitResult(
                                        sl.iter().map(|s| s.to_string()).collect(),
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error when reading line from fifo:\n{:#?}", e);
                            {
                                let mut command_to_channel = command_to_channel.lock().await;

                                for sender in (*command_to_channel).values_mut() {
                                    sender
                                        .send(Err(Error::FifoReadingAborted))
                                        .await
                                        .map_err(|e| Error::ChannelSendError(Box::new(e)))?;
                                }
                                (*command_to_channel).clear();
                            }

                            return Err(Error::FifoReadError(e));
                        }
                    }
                }
                tracing::debug!("Stopped listening on FIFO");
                {
                    let mut command_to_channel = command_to_channel.lock().await;

                    for sender in (*command_to_channel).values_mut() {
                        sender
                            .send(Err(Error::FifoReadingEnded))
                            .await
                            .map_err(|e| Error::ChannelSendError(Box::new(e)))?;
                    }
                    (*command_to_channel).clear();
                }
                session.close().await?;
            }
            let result: Result<(), Error> = Ok(());
            result
        });

        Ok(TmuxCommandRunner {
            ssh_destination,
            tmux_tmp_dir,
            tmux_socket,
            tmux_socket_filename,
            tmux_session,
            command_to_channel,
            join_handle: remote_child_join_handle,
            quit_channel_sender,
        })
    }

    /// run a new command in the tmux session
    ///
    /// # Errors
    ///
    /// this fails on SSH errors or tmux exit status that is not success
    pub async fn run_command(&mut self, command: &str) -> Result<CommandResult, Error> {
        let (sender, receiver) = postage::oneshot::channel();
        let command_id = uuid::Uuid::new_v4();
        {
            let mut command_to_channel = self.command_to_channel.lock().await;
            command_to_channel.insert(command_id, sender);
        }
        let session =
            openssh::Session::connect(&self.ssh_destination, openssh::KnownHosts::Strict).await?;

        let tmux = Tmux::new(self.tmux_socket.to_owned(), &session);

        let window = tmux.new_window(&self.tmux_session).await?;

        let pane = window.default_pane();

        tmux.set_option(
            TmuxOptionScope::Pane(pane.clone()),
            "@pane_command_id",
            &command_id.to_string(),
        )
        .await?;

        tmux.pipe_pane_output_to(
            &pane,
            &format!(
                "cat >> {}/{}_#S:#I.#P.log",
                self.tmux_tmp_dir, self.tmux_socket_filename
            ),
        )
        .await?;

        tmux.respawn_pane(&window.default_pane(), command).await?;
        Ok(CommandResult {
            channel_receiver: receiver,
        })
    }

    /// this must be awaited at the end, when the command runner is no
    /// longer needed. If any commands are still in progress they will
    /// not be cleaned up on the server. Await all the futures for commands
    /// started on this runner before awaiting this to avoid that issue.
    ///
    /// # Errors
    ///
    /// this fails if we either could not send the quit signal to the background task
    /// or if the background task itself exits with an error
    pub async fn close(mut self) -> Result<(), Error> {
        tracing::debug!("Closing down async command runner");
        self.quit_channel_sender
            .send(())
            .await
            .map_err(|e| crate::Error::QuitChannelSendError(Box::new(e)))?;
        let res = self.join_handle.await?;
        res?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tracing_test::traced_test;

    #[traced_test]
    #[tokio::test]
    async fn test() -> Result<(), Box<dyn std::error::Error>> {
        dotenv::dotenv()?;
        let ssh_user = std::env::var("TEST_SSH_USER")?;
        let ssh_host = std::env::var("TEST_SSH_HOST")?;

        let tmux_tmp_dir = std::env::var("TEST_TMUX_TMP_DIR")?;

        let tmux_socket_filename = std::env::var("TEST_SOCKET_FILENAME")?;

        let tmux_session_name = TmuxSessionName(std::env::var("TEST_TMUX_SESSION_NAME")?);

        let mut tmux_command_runner = TmuxCommandRunner::new(
            format!("{}@{}", ssh_user, ssh_host),
            tmux_tmp_dir,
            tmux_socket_filename,
            tmux_session_name,
        )
        .await?;

        let command_result1 = tmux_command_runner
            .run_command("du -xh / | sort -h | tail -n 20")
            .await?;

        let command_result2 = tmux_command_runner
            .run_command("sleep 5 && echo DONE")
            .await?;

        let (status1, output1) = command_result1.await?;

        println!(
            "Command 1 exited with status {} and output:\n{}",
            status1, output1
        );

        let (status2, output2) = command_result2.await?;

        println!(
            "Command 2 exited with status {} and output:\n{}",
            status2, output2
        );

        tmux_command_runner.close().await?;

        Ok(())
    }
}
