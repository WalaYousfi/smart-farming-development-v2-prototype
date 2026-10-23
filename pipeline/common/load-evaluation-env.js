// Loads evaluation settings for JavaScript producers



import fs from "node:fs";
import path from "node:path";

export function loadEvaluationEnv() {
  if (process.env.APP_ENV !== "evaluation") return;

  const file = path.resolve(process.cwd(), ".env.evaluation");

  if (!fs.existsSync(file)) {
    throw new Error(`Missing configuration: ${file}`);
  }

  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);

    if (match && !Object.hasOwn(process.env, match[1])) {
      process.env[match[1]] = match[2].replace(/^['"]|['"]$/g, "");
    }
  }

  if (process.env.MINIO_BUCKET !== "smart-farming-100000-records-evaluation") {
    throw new Error("Incorrect evaluation bucket");
  }
}
