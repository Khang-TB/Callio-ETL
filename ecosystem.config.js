const isWindows = process.platform === "win32";

module.exports = {
  apps: [
    {
      name: "callio-etl",
      script: "callio_etl",
      interpreter: isWindows ? "python" : "python3",
      interpreter_args: "-m",
      args: "--mode daemon",
      cwd: __dirname,
      env_file: ".env",
      env: {
        PYTHONUNBUFFERED: "1",
      },
      autorestart: true,
      max_restarts: 5,
      restart_delay: 5000,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
