const humanizeMs = (ms) => {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  }

  if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  }

  return `${seconds}s`;
};

const logTime = (startTime) => {
  return ` | ${humanizeMs(new Date().getTime() - startTime)}`;
};

module.exports = {
  humanizeMs,
  logTime,
};
