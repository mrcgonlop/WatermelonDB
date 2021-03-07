/**
 * @format
 */

import {AppRegistry, LogBox} from 'react-native';
import App from './App';
import {name as appName} from './app.json';

import {Database} from '@nozbe/watermelondb';
import SQLiteAdapter from '@nozbe/watermelondb/adapters/sqlite';
import {blogSchema} from './src/model/schema';
import {Post} from './src/model/Post.model.js';
import {Comment} from './src/model/Comment.model';
import {Blog} from './src/model/Blog.model';

// The example passes WatermelonDB objects as navigaiton params, these objects contain functions which triggers this warning.
// See more: https://reactnavigation.org/docs/troubleshooting/#i-get-the-warning-non-serializable-values-were-found-in-the-navigation-state
LogBox.ignoreLogs([
  'Non-serializable values were found in the navigation state',
]);

const adapter = new SQLiteAdapter({
  schema: blogSchema,
  dbName: 'WatermelonDemo', // optional database name or file system path
  // migrations, // optional migrations
  synchronous: true, // synchronous mode only works on iOS. improves performance and reduces glitches in most cases, but also has some downsides - test with and without it
  // experimentalUseJSI: true, // experimental JSI mode, use only if you're brave
});

export const database = new Database({
  adapter,
  modelClasses: [Post, Comment, Blog],
  actionsEnabled: true,
});

// const postsCollection = database.collections.get('posts');
// (async () => {
//   const allPosts = await postsCollection.query().fetch();
//
//   await database.action(async () => {
//     try {
//       const post = await postsCollection.create((post) => {
//         post.title = 'bang';
//         post.body = 'nice post';
//         post.isPinned = false;
//       });
//       console.log({post});
//     } catch (e) {
//       console.log(e);
//     }
//     // const post = await postsCollection.find('abcef');
//   });
//   console.log({allPosts});
// })();

AppRegistry.registerComponent(appName, () => App);
