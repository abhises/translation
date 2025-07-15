const AwsTranslateManager = require("./service/translationManger");

(async () => {
  const manager = new AwsTranslateManager({
    region: "us-east-1",
    bucket: "abhisesbucket",
    jsonKey: "input/translations.json",
    csvKey: "costume_terminology.csv",
    customDictionaryName: "my-translate-overrides",
    defaultTargetLanguage: "fr",
  });

  try {
    const result = await manager.translateText("My name is Abhises");
    console.log("✅ Single translation:", result);

    const inputS3Uri =
      "s3://abhisesbucket/logs/auth/login/12345/10-07-2025.log";
    const outputS3Uri = "s3://abhisesbucket/output/login.json";

    const jobId = await manager.startBulkTranslation(inputS3Uri, outputS3Uri);
    console.log("🚀 Bulk job complete. Job ID:", jobId);

    await manager.loadCustomDictionaryJson();
    await manager.addOrUpdateDictionaryEntry("hello", "fr", "bonjour");
    await manager.addOrUpdateDictionaryEntry("world", "fr", "monde");

    await manager.saveCustomDictionaryJson();
    await manager.convertJsonToAwsCsvAndUpload("fr");
    await manager.importDictionaryToAwsTranslate("fr");

    await manager.listBucketFiles();
  } catch (err) {
    console.error("❌ Translation failed:", err.message);
  }
})();
