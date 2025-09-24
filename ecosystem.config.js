const path = require("path");

const pythonInterpreter =
  process.env.PM2_PYTHON || process.env.PYTHON_PATH || "python";

module.exports = {
  apps: [
    {
      name: "callio-etl",
      script: path.join(__dirname, "callio_etl", "__main__.py"),
      interpreter: pythonInterpreter,
      args: "--mode daemon",
      cwd: __dirname,
      env_file: path.join(__dirname, ".env"),
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
