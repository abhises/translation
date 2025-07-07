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

dotenvConfig();

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
    this.jsonKey = config.jsonKey; // JSON array in S3
    this.csvKey = config.csvKey || "costume_terminology.csv"; // Target for AWS CSV
    this.generatedCsvKey = `output/${this.csvKey}_${Date.now()}.csv`; // dynamic file name
    this.customDictionaryName =
      config.customDictionaryName || "my-translate-overrides"; // Terminology name
    this.iamRoleArn = config.iamRoleArn;
    this.defaultTargetLanguage = config.defaultTargetLanguage || "fr";

    this._s3 = null;
    this._translate = null;

    this._dictionaryEntries = []; // Internal memory store
  }

  get s3() {
    if (!this._s3)
      this._s3 = new S3Client({
        region: this.region,
        credentials: {
          accessKeyId: process.env.S3_ACCESS_KEY,
          secretAccessKey: process.env.S3_SECRET_KEY,
        },
      });
    return this._s3;
  }

  get translate() {
    if (!this._translate)
      this._translate = new TranslateClient({
        region: this.region,
        credentials: {
          accessKeyId: process.env.AWS_TRANSLATE_KEY,
          secretAccessKey: process.env.AWS_TRANSLATE_SECRET,
        },
      });
    return this._translate;
  }

  /** ========== 1. BASIC TRANSLATION ========== */
  async translateText(text, targetLang = this.defaultTargetLanguage) {
    const command = new TranslateTextCommand({
      Text: text,
      SourceLanguageCode: "en",
      TargetLanguageCode: targetLang,
    });

    const result = await this.translate.send(command);
    return result.TranslatedText;
  }

  //   async startBulkTranslation(
  //     inputS3Uri,
  //     outputS3Uri,
  //     targetLangs = [this.defaultTargetLanguage]
  //   ) {
  //     if (!this.iamRoleArn) {
  //       throw new Error("IAM role ARN is required for bulk translation.");
  //     }

  //     const params = {
  //       JobName: `bulk-translate-${Date.now()}`,
  //       InputDataConfig: {
  //         S3Uri: inputS3Uri,
  //         ContentType: "application/json",
  //       },
  //       OutputDataConfig: {
  //         S3Uri: outputS3Uri,
  //       },
  //       DataAccessRoleArn: this.iamRoleArn,
  //       TargetLanguageCodes: targetLangs,
  //     };

  //     if (this.customDictionaryName) {
  //       params.TerminologyNames = [this.customDictionaryName];
  //     }

  //     const command = new StartTextTranslationJobCommand(params);
  //     const res = await this.translate.send(command);
  //     return res.JobId;
  //   }

  async startBulkTranslation(
    inputS3Uri,
    outputS3Uri,
    targetLangs = [this.defaultTargetLanguage]
  ) {
    if (!this.iamRoleArn) {
      console.log(
        "⚠️ IAM Role not provided. Using manual translation instead..."
      );

      // 1. Download input file from S3
      const inputKey = inputS3Uri.replace(`s3://${this.bucket}/`, "");

      const getCmd = new GetObjectCommand({
        Bucket: this.bucket,
        Key: inputKey,
      });
      const response = await this.s3.send(getCmd);
      const jsonText = await streamToString(response.Body);
      const items = JSON.parse(jsonText); // Should be array of text strings or objects with `Text` or `text` or `source` field

      // 2. Translate each item
      const translated = [];
      for (const item of items) {
        const text = item?.Text || item?.text || item?.source;

        if (!text) {
          console.warn("⚠️ Skipping item with no text:", item);
          continue;
        }

        const resultsPerLang = {};
        for (const lang of targetLangs) {
          const command = new TranslateTextCommand({
            Text: text,
            SourceLanguageCode: "en",
            TargetLanguageCode: lang,
          });
          const result = await this.translate.send(command);
          resultsPerLang[lang] = result.TranslatedText;
        }

        translated.push({
          original: text,
          ...resultsPerLang,
        });
      }

      // 3. Save result to S3
      const outputKey = outputS3Uri.replace(`s3://${this.bucket}/`, "");
      const uploadCmd = new PutObjectCommand({
        Bucket: this.bucket,
        Key: outputKey,
        Body: JSON.stringify(translated, null, 2),
        ContentType: "application/json",
      });

      await this.s3.send(uploadCmd);
      console.log("✅ Manual translation complete. File saved to:", outputKey);
      return "manual-job-done";
    }

    // ✅ IAM Role present, use real AWS Translate batch job
    const params = {
      JobName: `bulk-translate-${Date.now()}`,
      InputDataConfig: {
        S3Uri: inputS3Uri,
        ContentType: "application/json", // Should be plain text or supported format
      },
      OutputDataConfig: {
        S3Uri: outputS3Uri,
      },
      DataAccessRoleArn: this.iamRoleArn,
      TargetLanguageCodes: targetLangs,
    };

    if (this.customDictionaryName) {
      params.TerminologyNames = [this.customDictionaryName];
    }

    const command = new StartTextTranslationJobCommand(params);
    const res = await this.translate.send(command);
    return res.JobId;
  }

  async loadCustomDictionaryJson() {
    const command = new GetObjectCommand({
      Bucket: this.bucket,
      Key: this.jsonKey,
    });

    const response = await this.s3.send(command);
    const jsonStr = await streamToString(response.Body);
    this._dictionaryEntries = JSON.parse(jsonStr);
    return this._dictionaryEntries;
  }

  async addOrUpdateDictionaryEntry(source, targetLang, translation) {
    const idx = this._dictionaryEntries.findIndex(
      (e) => e.source === source && e.language === targetLang
    );

    if (idx !== -1) {
      this._dictionaryEntries[idx].translation = translation;
      return `Updated entry for source: "${source}" and language: "${targetLang}"`;
    } else {
      this._dictionaryEntries.push({
        source,
        language: targetLang,
        translation,
      });
      return `Added new entry for source: "${source}" and language: "${targetLang}"`;
    }
  }
  async deleteDictionaryEntry(source, targetLang) {
    const beforeCount = this._dictionaryEntries.length;
    this._dictionaryEntries = this._dictionaryEntries.filter(
      (e) => !(e.source === source && e.language === targetLang)
    );
    const afterCount = this._dictionaryEntries.length;
    if (afterCount < beforeCount) {
      return `Deleted entry for source: "${source}" and language: "${targetLang}"`;
    } else {
      return `No entry found for source: "${source}" and language: "${targetLang}"`;
    }
  }

  async saveCustomDictionaryJson() {
    const body = JSON.stringify(this._dictionaryEntries, null, 2);
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: this.jsonKey,
      Body: body,
      ContentType: "application/json",
    });
    return await this.s3.send(command);
  }
  async convertJsonToAwsCsvAndUpload(targetLang = this.defaultTargetLanguage) {
    const rows = this._dictionaryEntries
      .filter((e) => e.language === targetLang)
      .map((e) => ({
        SourceText: e.source,
        [targetLang]: e.translation,
      }));

    const csv = parse(rows, { fields: ["SourceText", targetLang] });

    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: this.generatedCsvKey, // ⬅️ changed from this.csvKey
      Body: csv,
      ContentType: "text/csv",
    });

    return await this.s3.send(command);
  }

  async importDictionaryToAwsTranslate(
    targetLang = this.defaultTargetLanguage
  ) {
    if (!this.customDictionaryName) {
      throw new Error("Custom dictionary name is required.");
    }

    const getObjectCmd = new GetObjectCommand({
      Bucket: this.bucket,
      Key: this.csvKey,
    });

    const response = await this.s3.send(getObjectCmd);

    const streamToBuffer = async (stream) => {
      return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks)));
      });
    };

    const csvBuffer = await streamToBuffer(response.Body);

    const params = {
      Name: this.customDictionaryName,
      MergeStrategy: "OVERWRITE",
      Description: `Overrides for ${targetLang}`,
      TerminologyData: {
        File: csvBuffer,
        Format: "CSV",
      },
    };

    const command = new ImportTerminologyCommand(params);
    return await this.translate.send(command);
  }
}

(async () => {
  const manager = new AwsTranslateManager({
    region: "us-east-1", // ✅ Your actual S3 region
    bucket: "abhisesbucket",
    jsonKey: "input/translations.json",
    csvKey: "costume_terminology.csv",
    customDictionaryName: "my-translate-overrides", // Make sure this is set
    // iamRoleArn: "arn:aws:iam::701253760804:role/TranslateS3Role",
    defaultTargetLanguage: "fr",
  });

  try {
    const result = await manager.translateText("my name is abhises");
    console.log("✅ Translated:", result);

    const inputS3Uri = "s3://abhisesbucket/input/translations.json"; // make sure this file exists
    const outputS3Uri = "s3://abhisesbucket/output/translations.csv"; // should be a directory

    const jobId = await manager.startBulkTranslation(inputS3Uri, outputS3Uri);
    console.log("🚀 Bulk translation job started. Job ID:", jobId);

    const dictionary = await manager.loadCustomDictionaryJson();
    console.log("✅ Loaded dictionary:", dictionary);

    console.log(manager.addOrUpdateDictionaryEntry("hello", "fr", "bonjour"));
    console.log(manager.addOrUpdateDictionaryEntry("world", "fr", "monde"));

    console.log("Before delete:", manager._dictionaryEntries);

    // Delete an entry
    console.log(manager.deleteDictionaryEntry("hello", "fr"));

    console.log("After delete:", manager._dictionaryEntries);

    // Try deleting a non-existing entry
    console.log(manager.deleteDictionaryEntry("nonexistent", "fr"));

    const testingSavingCostume = await manager.saveCustomDictionaryJson();
    console.log(
      "✅ Dictionary saved successfully. ETag:",
      testingSavingCostume.ETag
    );
    const res = await manager.convertJsonToAwsCsvAndUpload();
    console.log("✅ CSV uploaded successfully. ETag:", res.ETag);
    const testImportDictionaryToAwsTranslate =
      await manager.importDictionaryToAwsTranslate();
    console.log("✅ Dictionary imported:", testImportDictionaryToAwsTranslate);
    // console.log(manager._dictionaryEntries);
  } catch (error) {
    console.error("❌ Translation failed:", error.message);
  }
})();
