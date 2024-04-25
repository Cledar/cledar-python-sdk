module.exports = {
  plugins: [
    [
      '@semantic-release/commit-analyzer',
      {
        releaseRules: [
          {type: "build", release: "patch"},
          {type: "ci", release: "patch"},
          {type: "docs", release: false},
          {type: "refactor", release: "patch"},
          {type: "style", release: "patch"},
          {type: "test", release: "patch"},
          {scope: "no-release", release: false}
        ]
      }
    ],
    '@semantic-release/git',
    '@semantic-release/gitlab',
    [
      '@semantic-release/exec',
      {
        publishCmd: 'echo ${nextRelease.version} > .next_version'
      }
    ]
  ],
  branches: [
    'master'
  ]
};