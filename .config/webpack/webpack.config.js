const path = require('path');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const ESLintPlugin = require('eslint-webpack-plugin');
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');

const config = (env) => ({
  cache: {
    type: 'filesystem',
    buildDependencies: {
      config: [__filename],
    },
  },

  context: path.join(process.cwd(), 'src'),

  devtool: env.production ? 'source-map' : 'eval-source-map',

  entry: {
    module: './module.ts',
  },

  externals: [
    'lodash',
    'jquery',
    'moment',
    'slate',
    'prismjs',
    'slate-plain-serializer',
    'slate-react',
    'react',
    'react-dom',
    'react-redux',
    'redux',
    'rxjs',
    'react-router-dom',
    'd3',
    'angular',
    '@grafana/ui',
    '@grafana/runtime',
    '@grafana/data',
  ],

  mode: env.production ? 'production' : 'development',

  module: {
    rules: [
      {
        exclude: /(node_modules)/,
        test: /\.[tj]sx?$/,
        use: {
          loader: 'swc-loader',
          options: {
            jsc: {
              target: 'es2015',
              loose: false,
              parser: {
                syntax: 'typescript',
                tsx: true,
                decorators: false,
                dynamicImport: true,
              },
            },
          },
        },
      },
      {
        test: /\.(png|jpe?g|gif|svg)$/,
        type: 'asset/resource',
        generator: {
          filename: 'img/[name].[hash:8][ext]',
        },
      },
      {
        test: /\.(woff|woff2|eot|ttf|otf)(\?v=\d+\.\d+\.\d+)?$/,
        type: 'asset/resource',
        generator: {
          filename: 'fonts/[name].[hash:8][ext]',
        },
      },
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader'],
      },
      {
        test: /\.s[ac]ss$/,
        use: ['style-loader', 'css-loader', 'sass-loader'],
      },
    ],
  },

  output: {
    clean: true,
    filename: '[name].js',
    library: {
      type: 'amd',
    },
    path: path.resolve(process.cwd(), 'dist'),
    publicPath: '/',
  },

  plugins: [
    new CopyWebpackPlugin({
      patterns: [
        { from: '../README.md', to: '.', force: true },
        { from: '../LICENSE', to: '.', force: true },
        { from: '../CHANGELOG.md', to: '.', force: true },
        { from: '../plugin.json', to: '.' },
        { from: '../img', to: 'img', noErrorOnMissing: true },
      ],
    }),

    new ForkTsCheckerWebpackPlugin({
      async: Boolean(env.development),
      issue: {
        include: [{ file: '**/*.{ts,tsx}' }],
      },
      typescript: { configFile: path.join(process.cwd(), 'tsconfig.json') },
    }),

    new ESLintPlugin({
      extensions: ['.ts', '.tsx'],
      lintDirtyModulesOnly: Boolean(env.development),
    }),
  ],

  resolve: {
    extensions: ['.js', '.jsx', '.ts', '.tsx'],
    modules: [path.resolve(process.cwd(), 'src'), 'node_modules'],
    unsafeCache: true,
  },
});

module.exports = config;
