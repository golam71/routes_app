// apply.js  (was: src/scripts/apply-geocodes-from-file.js)
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { createClient } from "@supabase/supabase-js";

// load env
dotenv.config({ path: ".env" }); // change if needed

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

function loadGeocodeMap() {
  const filePath = path.join("src", "data", "route-geocodes.json");
  if (!fs.existsSync(filePath)) {
    console.error(`File not found: ${filePath}`);
    process.exit(1);
  }

  const raw = fs.readFileSync(filePath, "utf8");
  let arr;
  try {
    arr = JSON.parse(raw);
  } catch (e) {
    console.error("Invalid JSON in route-geocodes.json:", e.message);
    process.exit(1);
  }

  if (!Array.isArray(arr)) {
    console.error("route-geocodes.json must be an array of { name, lat, lon }");
    process.exit(1);
  }

  const map = new Map();
  for (const item of arr) {
    if (!item || !item.name) continue;
    map.set(item.name.trim(), { lat: item.lat, lon: item.lon });
  }

  console.log(`Loaded ${map.size} geocoded stops from route-geocodes.json`);
  return map;
}

async function main() {
  const geoMap = loadGeocodeMap();

  console.log("Fetching buses from bus_data...");

  // assuming bus_data has columns: id, routes (currently text[] or jsonb)
  const { data: buses, error } = await supabase
    .from("bus_data")
    .select("id, routes");

  if (error) {
    console.error("Error loading bus_data:", error.message);
    process.exit(1);
  }

  console.log(`Found ${buses.length} buses.`);

  const updates = [];

  for (const bus of buses) {
    if (!Array.isArray(bus.routes)) continue;

    // new value to store into `routes`
    const newRoutes = bus.routes.map((rawName) => {
      const name = (rawName || "").trim();
      const geo = geoMap.get(name) || null;

      if (!geo) {
        console.warn(`No geocode found for stop "${name}"`);
        return { name, lat: null, lon: null };
      }

      return {
        name,
        lat: geo.lat,
        lon: geo.lon,
      };
    });

    updates.push({
      id: bus.id,
      routes: newRoutes, // <-- overwrite routes with [{name,lat,lon}, ...]
    });
  }

  console.log(`Prepared updates for ${updates.length} buses.`);

  // chunked upsert
  const chunkSize = 100;
  for (let i = 0; i < updates.length; i += chunkSize) {
    const chunk = updates.slice(i, i + chunkSize);

    const { error: upsertError } = await supabase
      .from("bus_data")
      .upsert(chunk, {
        onConflict: "id", // assumes id is PK/unique
      });

    if (upsertError) {
      console.error("Supabase upsert error:", upsertError.message);
      process.exit(1);
    }

    console.log(
      `Upserted ${chunk.length} rows (${Math.min(
        i + chunk.length,
        updates.length
      )}/${updates.length})`
    );
  }

  console.log("Done writing geocoded routes into bus_data.routes.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

