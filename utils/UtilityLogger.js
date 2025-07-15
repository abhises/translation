const logConfig = require("../configs/LogRoutes");
const fs = require("fs");
const path = require("path");
require("dotenv").config(); // ✅ Load environment variables first
const moment = require("moment");

const localLogRoute = path.join(process.cwd(), "logs");
const isLocal = process.env.NODE_ENV === "local";

class Logger {
  static isEnabled() {
    return !!process.env.LOGGING_ENABLED && process.env.LOGGING_ENABLED !== "0";
  }

  static getRouteByFlag(flag) {
    for (const category of Object.values(logConfig)) {
      const parentMeta = {
        retention: category.retention,
        category: category.category,
        description: category.description,
      };
      const found = category.logs.find((log) => log.flag === flag);
      if (found) {
        return {
          ...parentMeta,
          ...found,
        };
      }
    }
    return {
      retention: "unknown",
      category: "unknown",
      description: "⚠️ MISSING LOG ROUTE DEFINITION",
      path: `missingLogRoutes/${flag}/${moment(Date.now()).format("LL")}.log`,
      PCI_compliance: false,
      critical: true,
    };

    // throw new Error(`Log route for flag "${flag}" not found.`);
  }

  static writeLog({ flag, data = {}, action, critical = false, message = "" }) {
    if (!Logger.isEnabled()) return;

    const route = Logger.getRouteByFlag(flag);
    const finalCritical = critical || route.critical;

    const logEntry = {
      timestamp: new Date().toISOString(),
      flag,
      action,
      message,
      critical: finalCritical,
      data,
      retention: route.retention,
      PCI_compliance: route.PCI_compliance,
      description: route.description,
      category: route.category,
    };

    if (Logger.isConsoleEnabled()) {
      console.log(`[Logger flag=${flag}]`, JSON.stringify(logEntry, null, 2));
    }

    const logPath = Logger.resolvePath(route.path, data);

    // 🛑 If path resolution fails, log the error as a separate fallback entry
    if (!logPath) {
      Logger.writeToLocal(
        `fallback/system_error/missing_path_${flag}_${Date.now()}.log`,
        {
          ...logEntry,
          flag: "system_error",
          message: `Missing required path variables for flag "${flag}"`,
        }
      );
      return;
    }

    const criticalPath = path.posix.join("critical", logPath);

    if (isLocal) {
      Logger.writeToLocal(logPath, logEntry);
      if (finalCritical) Logger.writeToLocal(criticalPath, logEntry);
    } else {
      Logger.writeToS3(logPath, logEntry);
      if (finalCritical) Logger.writeToS3(criticalPath, logEntry);
    }

    if (finalCritical) {
      Logger.notifySlack(logEntry);
    }
  }

  static writeLogs(logArray) {
    if (!Array.isArray(logArray)) {
      throw new Error("writeLogs expects an array of log entries");
    }
    for (const log of logArray) {
      Logger.writeLog(log);
    }
  }

  static resolvePath(template, data) {
    let logPath = template;

    const placeholders = Array.from(logPath.matchAll(/\{([^}]+)\}/g)).map(
      (m) => m[1]
    );

    for (const placeholder of placeholders) {
      const [key, format] = placeholder.split(":").map((s) => s.trim());

      if (!(key in data)) {
        console.error(
          `[Logger] ❌ Missing key "${key}" for template "${template}"`
        );
        return null; // Let writeLog handle fallback
      }

      let replacement = data[key];

      switch (format) {
        case "DD-MM-YYYY":
          const date = new Date(replacement);
          replacement =
            `${String(date.getDate()).padStart(2, "0")}-` +
            `${String(date.getMonth() + 1).padStart(2, "0")}-` +
            `${date.getFullYear()}`;
          break;

        case "UID":
          replacement = String(replacement);
          break;

        default:
          replacement = String(replacement);
          break;
      }

      logPath = logPath.replace(`{${placeholder}}`, replacement);
    }

    return logPath;
  }

  static isConsoleEnabled() {
    return (
      process.env.NODE_ENV === "local" &&
      process.env.LOGGING_CONSOLE_ENABLED === "1"
    );
  }

  static writeToLocal(relativePath, logEntry) {
    const fullPath = path.join(localLogRoute, relativePath);
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });
    fs.appendFileSync(fullPath, JSON.stringify(logEntry) + "\n");
  }

  static writeToS3(relativePath, logEntry) {
    // Lazy load AWS SDK only when needed
    const AWS = require("aws-sdk");

    const S3_BUCKET = process.env.S3_BUCKET || "";
    const S3_ROOT_PREFIX = process.env.S3_ROOT_PREFIX || "logs/";

    const s3 = new AWS.S3();

    const s3Key = path.posix.join(S3_ROOT_PREFIX, relativePath);
    const Body = JSON.stringify(logEntry) + "\n";

    const params = {
      Bucket: S3_BUCKET,
      Key: s3Key,
      Body,
    };

    s3.putObject(params)
      .promise()
      .catch((err) => {
        console.error(`[Logger] Failed to write log to S3: ${err.message}`);
      });
  }

  static notifySlack(logEntry) {
    // Placeholder: implement your Slack notifier
    console.log(`[Logger] 🔔 Critical log posted to Slack:`, logEntry);
  }
}

module.exports = Logger;
