const consoleMessageWithOra = (message, spinner) => {
  spinner.text = message.trim();
  spinner.render();
};

module.exports = {
  consoleMessageWithOra,
};
