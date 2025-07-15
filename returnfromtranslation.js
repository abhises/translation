import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  TranslateClient,
  TranslateTextCommand,
  ImportTerminologyCommand,
  StartTextTranslationJobCommand,
} from "@aws-sdk/client-translate";
import { parse } from "json2csv";
import { config as dotenvConfig } from "dotenv";

import SafeUtils from "./SafeUtils";
import ErrorHandler from "./ErrorHandler";
import Logger from "./UtilityLogger";
import DateTime from "./DateTime"; // assumed

dotenvConfig();

function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    stream.on("error", reject);
  });
}

export default class AwsTranslateManager {
  constructor(config = {}) {
    const validated = SafeUtils.sanitizeValidate({
      region: {
        value: config.region,
        type: "string",
        required: false,
        default: "ap-northeast-1",
      },
      bucket: { value: config.bucket, type: "string", required: true },
      jsonKey: { value: config.jsonKey, type: "string", required: true },
      csvKey: { value: config.csvKey, type: "string", required: true },
      customDictionaryName: {
        value: config.customDictionaryName,
        type: "string",
        required: true,
      },
      iamRoleArn: { value: config.iamRoleArn, type: "string", required: true },
      defaultTargetLanguage: {
        value: config.defaultTargetLanguage,
        type: "string",
        required: false,
        default: "fr",
      },
    });

    this.region = validated.region;
    this.bucket = validated.bucket;
    this.jsonKey = validated.jsonKey;
    this.csvKey = validated.csvKey;
    this.customDictionaryName = validated.customDictionaryName;
    this.iamRoleArn = validated.iamRoleArn;
    this.defaultTargetLanguage = validated.defaultTargetLanguage;

    this.s3 = new S3Client({
      region: this.region,
      credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY,
        secretAccessKey: process.env.S3_SECRET_KEY,
      },
    });

    this.translate = new TranslateClient({
      region: this.region,
      credentials: {
        accessKeyId: process.env.AWS_TRANSLATE_KEY,
        secretAccessKey: process.env.AWS_TRANSLATE_SECRET,
      },
    });

    this._dictionaryEntries = [];
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
            time: DateTime.now(),
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

    const command = new StartTextTranslationJobCommand({
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
    });

    if (Logger.isConsoleEnabled()) {
      console.log(
        "[Logger flag=bulk_translation]",
        JSON.stringify(
          {
            action: "startBulkTranslation",
            data: params,
            time: DateTime.now(),
          },
          null,
          2
        )
      );
    }

    const res = await this.translate.send(command);
    return res.JobId;
  }

  async loadCustomDictionaryJson() {
    const command = new GetObjectCommand({
      Bucket: this.bucket,
      Key: this.jsonKey,
    });
    try {
      const response = await this.s3.send(command);
      const jsonStr = await streamToString(response.Body);
      this._dictionaryEntries = JSON.parse(jsonStr);

      if (Logger.isConsoleEnabled()) {
        console.log(
          "[Logger flag=load_dictionary]",
          JSON.stringify(
            {
              action: "loadCustomDictionaryJson",
              data: { count: this._dictionaryEntries.length },
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }

      return this._dictionaryEntries;
    } catch (err) {
      ErrorHandler.add_error("Failed to load custom dictionary", err);
      Logger.writeLog({
        flag: "system_error",
        action: "loadCustomDictionaryJson",
        message: err.message,
        critical: true,
        data: { bucket: this.bucket, jsonKey: this.jsonKey },
      });
      throw new Error("Failed to load dictionary");
    }
  }

  addOrUpdateDictionaryEntry(source, targetLang, translation) {
    const params = SafeUtils.sanitizeValidate({
      source: { value: source, type: "string", required: true },
      targetLang: { value: targetLang, type: "string", required: true },
      translation: { value: translation, type: "string", required: true },
    });

    const idx = this._dictionaryEntries.findIndex(
      (e) => e.source === params.source && e.language === params.targetLang
    );

    if (idx !== -1) {
      this._dictionaryEntries[idx].translation = params.translation;
    } else {
      this._dictionaryEntries.push({
        source: params.source,
        language: params.targetLang,
        translation: params.translation,
      });
    }
  }

  deleteDictionaryEntry(source, targetLang) {
    const params = SafeUtils.sanitizeValidate({
      source: { value: source, type: "string", required: true },
      targetLang: { value: targetLang, type: "string", required: true },
    });

    this._dictionaryEntries = this._dictionaryEntries.filter(
      (e) => !(e.source === params.source && e.language === params.targetLang)
    );
  }

  async saveCustomDictionaryJson() {
    const body = JSON.stringify(this._dictionaryEntries, null, 2);
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: this.jsonKey,
      Body: body,
      ContentType: "application/json",
    });

    try {
      await this.s3.send(command);

      if (Logger.isConsoleEnabled()) {
        console.log(
          "[Logger flag=save_dictionary]",
          JSON.stringify(
            {
              action: "saveCustomDictionaryJson",
              data: { entries: this._dictionaryEntries.length },
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("Failed to save dictionary JSON", err);
      Logger.writeLog({
        flag: "system_error",
        action: "saveCustomDictionaryJson",
        message: err.message,
        critical: true,
        data: { bucket: this.bucket, jsonKey: this.jsonKey },
      });
      throw new Error("Failed to save dictionary JSON");
    }
  }

  async convertJsonToAwsCsvAndUpload(targetLang = this.defaultTargetLanguage) {
    const params = SafeUtils.sanitizeValidate({
      targetLang: {
        value: targetLang,
        type: "string",
        required: false,
        default: this.defaultTargetLanguage,
      },
    });

    const rows = this._dictionaryEntries
      .filter(
        (e) =>
          e.language === params.targetLang &&
          e.source &&
          e.translation &&
          e.source.trim() &&
          e.translation.trim()
      )
      .map((e) => ({
        SourceText: e.source.trim(),
        TargetText: e.translation.trim(),
      }));

    const csv = parse(rows, { fields: ["SourceText", "TargetText"] });

    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: this.csvKey,
      Body: Buffer.from(csv, "utf-8"),
      ContentType: "text/csv",
    });

    await this.s3.send(command);
  }

  async importDictionaryToAwsTranslate(
    targetLang = this.defaultTargetLanguage
  ) {
    const params = SafeUtils.sanitizeValidate({
      targetLang: {
        value: targetLang,
        type: "string",
        required: false,
        default: this.defaultTargetLanguage,
      },
    });

    const getCsv = new GetObjectCommand({
      Bucket: this.bucket,
      Key: this.csvKey,
    });
    const csvResponse = await this.s3.send(getCsv);
    const csvBuffer = await streamToString(csvResponse.Body);

    const command = new ImportTerminologyCommand({
      Name: this.customDictionaryName,
      MergeStrategy: "OVERWRITE",
      Description: `Overrides for ${params.targetLang}`,
      TerminologyData: {
        File: Buffer.from(csvBuffer, "utf-8"),
        Format: "CSV",
      },
    });

    await this.translate.send(command);
  }

  async autoTranslateMissing() {
    for (const entry of this._dictionaryEntries) {
      if (!entry.translation?.trim() && entry.source?.trim()) {
        try {
          const translated = await this.translateText(
            entry.source,
            entry.language || this.defaultTargetLanguage
          );
          entry.translation = translated;
        } catch (err) {
          ErrorHandler.add_error(`Failed to translate "${entry.source}"`, err);
        }
      }
    }
  }
}
