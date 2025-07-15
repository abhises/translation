const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
} = require("@aws-sdk/client-s3");

const {
  TranslateClient,
  TranslateTextCommand,
  ImportTerminologyCommand,
  StartTextTranslationJobCommand,
} = require("@aws-sdk/client-translate");

const { parse } = require("json2csv");
const dotenv = require("dotenv");
const SafeUtils = require("../utils/SafeUtils");
const ErrorHandler = require("../utils/ErrorHandler");
const Logger = require("../utils/UtilityLogger");
const DateTime = require("../utils/DateTime");
dotenv.config();

function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
}

class AwsTranslateManager {
  constructor(config = {}) {
    this.region = config.region || "us-east-1";
    this.bucket = config.bucket || "abhisebucket";
    this.jsonKey = config.jsonKey;
    this.csvKey = config.csvKey || "costume_terminology.csv";
    this.customDictionaryName =
      config.customDictionaryName || "my-translate-overrides";
    this.iamRoleArn = config.iamRoleArn;
    this.defaultTargetLanguage = config.defaultTargetLanguage || "fr";

    this._s3 = null;
    this._translate = null;
    this._dictionaryEntries = [];
  }

  get s3() {
    if (!this._s3) {
      this._s3 = new S3Client({
        region: this.region,
        credentials: {
          accessKeyId: process.env.S3_ACCESS_KEY,
          secretAccessKey: process.env.S3_SECRET_KEY,
        },
      });
    }
    return this._s3;
  }

  get translate() {
    if (!this._translate) {
      this._translate = new TranslateClient({
        region: this.region,
        credentials: {
          accessKeyId: process.env.AWS_TRANSLATE_KEY,
          secretAccessKey: process.env.AWS_TRANSLATE_SECRET,
        },
      });
    }
    return this._translate;
  }

  async translateText(text, targetLang = this.defaultTargetLanguage) {
    const params = SafeUtils.sanitizeValidate({
      text: { value: text, type: "string", required: true },
      targetLang: {
        value: targetLang,
        type: "string",
        required: false,
        default: this.defaultTargetLanguage,
      },
    });

    const command = new TranslateTextCommand({
      Text: params.text,
      SourceLanguageCode: "en",
      TargetLanguageCode: params.targetLang,
      ...(this.customDictionaryName && {
        TerminologyNames: [this.customDictionaryName],
      }),
    });

    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=translate_text]",
        JSON.stringify(
          {
            action: "translateText",
            data: params,
            time: new Date().toISOString(),
          },
          null,
          2
        )
      );
    }

    try {
      const result = await this.translate.send(command);
      return result.TranslatedText;
    } catch (err) {
      ErrorHandler.add_error("Failed to translate text", {
        text: params.text,
        error: err,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "translateText",
        message: err.message,
        critical: true,
        data: params,
      });

      throw new Error("Translation failed");
    }
  }

  async startBulkTranslation(
    inputS3Uri,
    outputS3Uri,
    targetLangs = [this.defaultTargetLanguage]
  ) {
    const params = SafeUtils.sanitizeValidate({
      inputS3Uri: { value: inputS3Uri, type: "string", required: true },
      outputS3Uri: { value: outputS3Uri, type: "string", required: true },
      targetLangs: {
        value: targetLangs,
        type: "array",
        required: false,
        default: [this.defaultTargetLanguage],
      },
    });

    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=bulk_translation]",
        JSON.stringify(
          {
            action: "startBulkTranslation",
            data: params,
            time: new Date().toISOString(),
          },
          null,
          2
        )
      );
    }

    // Handle manual fallback if IAM role is not available
    if (!this.iamRoleArn) {
      Logger.writeLog({
        flag: "manual_fallback",
        action: "startBulkTranslation",
        message: "IAM Role not provided, running manual translation",
        data: params,
      });

      try {
        const inputKey = params.inputS3Uri.replace(`s3://${this.bucket}/`, "");
        const getCmd = new GetObjectCommand({
          Bucket: this.bucket,
          Key: inputKey,
        });

        const response = await this.s3.send(getCmd);
        const rawText = await streamToString(response.Body);

        let items = [];
        try {
          const parsed = JSON.parse(rawText);
          items = Array.isArray(parsed)
            ? parsed
            : Array.isArray(parsed.translations)
            ? parsed.translations
            : [];

          if (!Array.isArray(items) || items.length === 0) {
            throw new Error(
              "Parsed JSON did not contain an array of translatable items."
            );
          }
        } catch {
          console.warn(
            "⚠️ Could not parse as JSON array. Falling back to line-by-line text."
          );
          items = rawText.split(/\r?\n/).filter((line) => line.trim() !== "");
        }

        const translated = [];

        for (const entry of items) {
          const text =
            typeof entry === "string"
              ? entry
              : entry?.Text || entry?.text || entry?.source;

          if (!text) {
            console.warn("⚠️ Skipping item with no translatable text:", entry);
            continue;
          }

          const resultsPerLang = {};
          for (const lang of params.targetLangs) {
            try {
              const result = await this.translate.send(
                new TranslateTextCommand({
                  Text: text,
                  SourceLanguageCode: "en",
                  TargetLanguageCode: lang,
                })
              );
              resultsPerLang[lang] = result.TranslatedText;
            } catch (translationError) {
              Logger.writeLog({
                flag: "translation_error",
                action: "manualTranslate",
                message: translationError.message,
                critical: false,
                data: { text, lang },
              });

              resultsPerLang[lang] = null;
            }
          }

          translated.push({ original: text, ...resultsPerLang });
        }

        const outputKey = params.outputS3Uri.replace(
          `s3://${this.bucket}/`,
          ""
        );
        const uploadCmd = new PutObjectCommand({
          Bucket: this.bucket,
          Key: outputKey,
          Body: JSON.stringify(translated, null, 2),
          ContentType: "application/json",
        });

        await this.s3.send(uploadCmd);

        return "manual-job-done";
      } catch (err) {
        ErrorHandler.add_error("Manual translation failed", {
          error: err,
          inputS3Uri: params.inputS3Uri,
          outputS3Uri: params.outputS3Uri,
        });

        Logger.writeLog({
          flag: "system_error",
          action: "manualTranslation",
          message: err.message,
          critical: true,
          data: params,
        });

        throw new Error(`Manual translation failed: ${err.message}`);
      }
    }

    // Proceed with AWS batch translation job
    const jobParams = {
      JobName: `bulk-translate-${Date.now()}`,
      InputDataConfig: {
        S3Uri: params.inputS3Uri,
        ContentType: "application/json",
      },
      OutputDataConfig: {
        S3Uri: params.outputS3Uri,
      },
      DataAccessRoleArn: this.iamRoleArn,
      TargetLanguageCodes: params.targetLangs,
      ...(this.customDictionaryName && {
        TerminologyNames: [this.customDictionaryName],
      }),
    };

    try {
      const command = new StartTextTranslationJobCommand(jobParams);
      const res = await this.translate.send(command);
      return res.JobId;
    } catch (err) {
      ErrorHandler.add_error("Failed to start AWS translation job", {
        error: err,
        jobParams,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "startBulkTranslation",
        message: err.message,
        critical: true,
        data: jobParams,
      });

      throw new Error("Bulk translation job failed");
    }
  }

  async loadCustomDictionaryJson() {
    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=load_dictionary]",
        JSON.stringify(
          {
            action: "loadCustomDictionaryJson",
            bucket: this.bucket,
            jsonKey: this.jsonKey,
            time: new Date().toISOString(),
          },
          null,
          2
        )
      );
    }

    try {
      const command = new GetObjectCommand({
        Bucket: this.bucket,
        Key: this.jsonKey,
      });

      const response = await this.s3.send(command);
      const jsonStr = await streamToString(response.Body);
      this._dictionaryEntries = JSON.parse(jsonStr);

      return this._dictionaryEntries;
    } catch (err) {
      const isMissingFile =
        err.name === "NoSuchKey" ||
        err.message.includes("The specified key does not exist");

      if (isMissingFile) {
        Logger.writeLog({
          flag: "dictionary_missing",
          action: "loadCustomDictionaryJson",
          message: `No dictionary file found at "${this.jsonKey}"`,
          data: {
            bucket: this.bucket,
            key: this.jsonKey,
          },
        });

        this._dictionaryEntries = [];
        return this._dictionaryEntries;
      }

      ErrorHandler.add_error("Failed to load dictionary JSON", {
        bucket: this.bucket,
        key: this.jsonKey,
        error: err,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "loadCustomDictionaryJson",
        message: err.message,
        critical: true,
        data: {
          bucket: this.bucket,
          key: this.jsonKey,
        },
      });

      throw new Error(`Failed to load dictionary: ${err.message}`);
    }
  }

  async addOrUpdateDictionaryEntry(source, targetLang, translation) {
    const params = SafeUtils.sanitizeValidate({
      source: { value: source, type: "string", required: true },
      targetLang: { value: targetLang, type: "string", required: true },
      translation: { value: translation, type: "string", required: true },
    });

    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=add_or_update_entry]",
        JSON.stringify(
          {
            action: "addOrUpdateDictionaryEntry",
            data: params,
            time: new Date().toISOString(),
          },
          null,
          2
        )
      );
    }

    try {
      const idx = this._dictionaryEntries.findIndex(
        (e) =>
          e.source.toLowerCase() === params.source.toLowerCase() &&
          e.language.toLowerCase() === params.targetLang.toLowerCase()
      );

      if (idx !== -1) {
        this._dictionaryEntries[idx].translation = params.translation;

        Logger.writeLog({
          flag: "dictionary_update",
          action: "addOrUpdateDictionaryEntry",
          message: `Updated translation for "${params.source}" in "${params.targetLang}"`,
          data: {
            index: idx,
            ...params,
          },
        });

        return `Updated entry for "${params.source}" → "${params.targetLang}"`;
      } else {
        this._dictionaryEntries.push({
          source: params.source,
          language: params.targetLang,
          translation: params.translation,
        });

        Logger.writeLog({
          flag: "dictionary_add",
          action: "addOrUpdateDictionaryEntry",
          message: `Added new translation for "${params.source}" in "${params.targetLang}"`,
          data: params,
        });

        return `Added new entry for "${params.source}" → "${params.targetLang}"`;
      }
    } catch (err) {
      ErrorHandler.add_error("Failed to add/update dictionary entry", {
        error: err,
        data: params,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "addOrUpdateDictionaryEntry",
        message: err.message,
        critical: true,
        data: params,
      });

      throw new Error("Could not add or update dictionary entry");
    }
  }

  async deleteDictionaryEntry(source, targetLang) {
    const params = SafeUtils.sanitizeValidate({
      source: { value: source, type: "string", required: true },
      targetLang: { value: targetLang, type: "string", required: true },
    });

    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=delete_entry]",
        JSON.stringify(
          {
            action: "deleteDictionaryEntry",
            data: params,
            time: new Date().toISOString(),
          },
          null,
          2
        )
      );
    }

    try {
      const before = this._dictionaryEntries.length;

      this._dictionaryEntries = this._dictionaryEntries.filter(
        (e) =>
          !(
            e.source.toLowerCase() === params.source.toLowerCase() &&
            e.language.toLowerCase() === params.targetLang.toLowerCase()
          )
      );

      const after = this._dictionaryEntries.length;

      if (after < before) {
        Logger.writeLog({
          flag: "dictionary_delete",
          action: "deleteDictionaryEntry",
          message: `Deleted entry for "${params.source}" in "${params.targetLang}"`,
          data: params,
        });
        return `Deleted entry for "${params.source}" → "${params.targetLang}"`;
      } else {
        Logger.writeLog({
          flag: "dictionary_not_found",
          action: "deleteDictionaryEntry",
          message: `No entry found for "${params.source}" in "${params.targetLang}"`,
          data: params,
        });
        return `No entry found for "${params.source}" → "${params.targetLang}"`;
      }
    } catch (err) {
      ErrorHandler.add_error("Failed to delete dictionary entry", {
        error: err,
        data: params,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "deleteDictionaryEntry",
        message: err.message,
        critical: true,
        data: params,
      });

      throw new Error("Could not delete dictionary entry");
    }
  }

  async saveCustomDictionaryJson() {
    const params = SafeUtils.sanitizeValidate({
      bucket: { value: this.bucket, type: "string", required: true },
      jsonKey: { value: this.jsonKey, type: "string", required: true },
      dictionaryEntries: {
        value: this._dictionaryEntries,
        type: "array",
        required: true,
      },
    });

    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=save_dictionary]",
        JSON.stringify(
          {
            action: "saveCustomDictionaryJson",
            bucket: params.bucket,
            key: params.jsonKey,
            totalEntries: params.dictionaryEntries.length,
            time: new Date().toISOString(),
          },
          null,
          2
        )
      );
    }

    try {
      const body = JSON.stringify(params.dictionaryEntries, null, 2);

      const command = new PutObjectCommand({
        Bucket: params.bucket,
        Key: params.jsonKey,
        Body: body,
        ContentType: "application/json",
      });

      const result = await this.s3.send(command);

      Logger.writeLog({
        flag: "dictionary_saved",
        action: "saveCustomDictionaryJson",
        message: "Custom dictionary saved to S3 successfully",
        data: {
          bucket: params.bucket,
          key: params.jsonKey,
          totalEntries: params.dictionaryEntries.length,
        },
      });

      return result;
    } catch (err) {
      ErrorHandler.add_error("Failed to save dictionary to S3", {
        error: err,
        bucket: params.bucket,
        key: params.jsonKey,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "saveCustomDictionaryJson",
        message: err.message,
        critical: true,
        data: {
          bucket: params.bucket,
          key: params.jsonKey,
        },
      });

      throw new Error("Could not save custom dictionary to S3");
    }
  }

  async convertJsonToAwsCsvAndUpload(targetLang = this.defaultTargetLanguage) {
    const params = SafeUtils.sanitizeValidate({
      bucket: { value: this.bucket, type: "string", required: true },
      csvKey: { value: this.csvKey, type: "string", required: true },
      targetLang: { value: targetLang, type: "string", required: false },
      dictionaryEntries: {
        value: this._dictionaryEntries,
        type: "array",
        required: true,
      },
    });

    const sourceLangCode = "en";
    const targetLangCode = params.targetLang.toLowerCase().trim();

    const rows = params.dictionaryEntries
      .filter((e) => e.language.toLowerCase() === targetLangCode)
      .map((e) => ({
        [sourceLangCode]: e.source,
        [targetLangCode]: e.translation,
      }));

    try {
      const csv = parse(rows, {
        fields: [sourceLangCode, targetLangCode],
        quote: '"',
      });

      if (Logger.isConsoleEnabled()) {
        console.log(
          "=== Generated CSV content ===\n",
          csv,
          "\n============================="
        );
      }

      const command = new PutObjectCommand({
        Bucket: params.bucket,
        Key: params.csvKey,
        Body: csv,
        ContentType: "text/csv",
      });

      const result = await this.s3.send(command);

      Logger.writeLog({
        flag: "csv_uploaded",
        action: "convertJsonToAwsCsvAndUpload",
        message: "CSV file uploaded to S3",
        data: { bucket: params.bucket, key: params.csvKey },
      });

      return result;
    } catch (err) {
      ErrorHandler.add_error("Failed to convert and upload CSV", {
        error: err,
        bucket: params.bucket,
        key: params.csvKey,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "convertJsonToAwsCsvAndUpload",
        message: err.message,
        critical: true,
        data: { bucket: params.bucket, key: params.csvKey },
      });

      throw new Error("Could not convert JSON to CSV and upload");
    }
  }

  async importDictionaryToAwsTranslate(
    targetLang = this.defaultTargetLanguage
  ) {
    const params = SafeUtils.sanitizeValidate({
      bucket: { value: this.bucket, type: "string", required: true },
      csvKey: { value: this.csvKey, type: "string", required: true },
      customDictionaryName: {
        value: this.customDictionaryName,
        type: "string",
        required: true,
      },
      targetLang: { value: targetLang, type: "string", required: false },
    });

    try {
      const getObjectCmd = new GetObjectCommand({
        Bucket: params.bucket,
        Key: params.csvKey,
      });

      const response = await this.s3.send(getObjectCmd);

      const streamToBuffer = (stream) =>
        new Promise((resolve, reject) => {
          const chunks = [];
          stream.on("data", (chunk) => chunks.push(chunk));
          stream.on("error", reject);
          stream.on("end", () => resolve(Buffer.concat(chunks)));
        });

      const csvBuffer = await streamToBuffer(response.Body);

      if (Logger.isConsoleEnabled()) {
        console.log(
          "=== CSV content being imported ===\n",
          csvBuffer.toString("utf-8"),
          "\n================================="
        );
      }

      const importParams = {
        Name: params.customDictionaryName,
        MergeStrategy: "OVERWRITE",
        Description: `Overrides for ${params.targetLang}`,
        TerminologyData: {
          File: csvBuffer,
          Format: "CSV",
        },
      };

      const command = new ImportTerminologyCommand(importParams);
      const result = await this.translate.send(command);

      Logger.writeLog({
        flag: "dictionary_imported",
        action: "importDictionaryToAwsTranslate",
        message: "Custom terminology imported successfully",
        data: {
          targetLang: params.targetLang,
          name: params.customDictionaryName,
        },
      });

      return result;
    } catch (err) {
      ErrorHandler.add_error("Failed to import custom terminology", {
        error: err,
        bucket: params.bucket,
        key: params.csvKey,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "importDictionaryToAwsTranslate",
        message: err.message,
        critical: true,
        data: { bucket: params.bucket, key: params.csvKey },
      });

      throw new Error("Failed to import dictionary to AWS Translate");
    }
  }

  async listBucketFiles() {
    const params = SafeUtils.sanitizeValidate({
      bucket: { value: this.bucket, type: "string", required: true },
    });

    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=list_bucket_files]",
        JSON.stringify(
          {
            action: "listBucketFiles",
            bucket: params.bucket,
            time: new Date().toISOString(),
          },
          null,
          2
        )
      );
    }

    try {
      const command = new ListObjectsV2Command({ Bucket: params.bucket });
      const result = await this.s3.send(command);

      Logger.writeLog({
        flag: "bucket_listing",
        action: "listBucketFiles",
        message: `Retrieved ${result.Contents?.length || 0} files`,
        data: {
          bucket: params.bucket,
          files: result.Contents?.map((f) => f.Key),
        },
      });

      if (Logger.isConsoleEnabled()) {
        console.log("🗂️ Files in bucket:");
        result.Contents?.forEach((file) => console.log("  -", file.Key));
      }

      return result.Contents;
    } catch (err) {
      ErrorHandler.add_error("Failed to list files in S3 bucket", {
        error: err,
        bucket: params.bucket,
      });

      Logger.writeLog({
        flag: "system_error",
        action: "listBucketFiles",
        message: err.message,
        critical: true,
        data: { bucket: params.bucket },
      });

      throw new Error("Could not list files in S3 bucket");
    }
  }
}

// 🧪 Runner
module.exports = AwsTranslateManager;
