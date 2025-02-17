module.exports = {
  bracketSpacing: true,
  printWidth: 80,
  semi: true,
  tabWidth: 2,
  trailingComma: 'es5',
  useTabs: false,
  overrides: [
    {
      files: '*.mo',
      options: {
        bracketSpacing: true,
        tabWidth: 4,
        plugins: ['prettier-plugin-motoko'],
        printWidth: 80,
      },
    },
  ],
};
