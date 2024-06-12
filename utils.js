const fs = require("fs");

function saveJson(json, filename) {
  try {
    const data = JSON.stringify(json);
    fs.writeFileSync(filename, data);
  } catch (e) {
    console.error("Failed to save JSON:", filename, e);
  }
}

function loadJson(filename, ignore) {
  try {
    let rawData = fs.readFileSync(filename);
    return JSON.parse(rawData);
  } catch (e) {
    if (!ignore) {
      console.error("Failed to load JSON:", filename, e);
    }
  }
  return null;
}

const getFilteredRows = (rows, filter) => {
  return rows.filter((row) =>
    Array.isArray(filter)
      ? filter.some((f) => recursiveFilter(f, row))
      : isObject(filter)
      ? recursiveFilter(filter, row)
      : false
  );
};

const isObject = function (o) {
  return o === Object(o) && !Array.isArray(o) && typeof o !== "function";
};

const recursiveFilter = (filter, obj) => {
  if (isObject(filter) && isObject(obj)) {
    return Object.keys(filter).every((key) =>
      recursiveFilter(filter[key], obj[key])
    );
  } else if (Array.isArray(filter) && Array.isArray(obj)) {
    return filter.every((value, index) => recursiveFilter(value, obj[index]));
  } else {
    return filter === obj;
  }
};

module.exports = {
  getFilteredRows,
  isObject,
  recursiveFilter,
  saveJson,
  loadJson,
};
