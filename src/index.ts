import fs from "fs";
import { Kafka } from "kafkajs";
import mongoose from "mongoose";
import path from "path";
import UserModel from "./models/UserSchema";
import { RedisClient } from "./redis/RedisClient";
require("dotenv").config();

interface LikeEvent {
  userId: string;
  points: string;
}

// Connect to the database
(async () => {
  const DB_URI = process.env.DB_URI || "mongodb://localhost:27017/proelevate";
  try {
    await mongoose.connect(DB_URI, {
      autoIndex: true,
    });
    console.log("Connected to DB");
  } catch (error) {
    console.log("Error connecting to DB", error);
  }
})();

// Implement the like_async route
(async () => {
  console.log("Waiting for new likes...");
  try {
    const KAFKA_HOST = process.env.KAFKA_HOST || "YOUR AIVEN KAFKA HOST";
    const kafkaClient = new Kafka({
      brokers: [KAFKA_HOST],
      ssl: {
        ca: [fs.readFileSync(path.resolve("./ca.pem"), "utf-8")],
      },
      sasl: {
        //@ts-ignore
        username: process.env.KAFKA_USERNAME || "YOUR AIVEN KAFKA USERNAME",
        password: process.env.KAFKA_PASSWORD || "YOUR AIVEN KAFKA PASSWORD",
        mechanism: "plain",
      },
    });

    const Consumer = kafkaClient.consumer({ groupId: "like-events" });
    await Consumer.connect();
    Consumer.on("consumer.connect", async () => {
      console.log("🟢 Kafka Consumer connected");
    });

    let batchedLikeEvents: LikeEvent[] = [];

    await Consumer.subscribe({ topic: "like_events", fromBeginning: true });

    await Consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        // Parse message
        const ParsedValue = message.value;
        if (!ParsedValue) {
          return;
        }
        const likeEvent: LikeEvent = JSON.parse(ParsedValue.toString());
        batchedLikeEvents.push(likeEvent);
        // Process batch of 50 messages
        if (batchedLikeEvents.length >= 5) {
          await processBatch(batchedLikeEvents);
          batchedLikeEvents = []; // Clear the batch
        }
      },
    });
  } catch (error) {
    console.error("Error:", error);
  }
})();

// async function processBatch(batch: LikeEvent[]) {
//   try {
//     // Fetch all unique user IDs from the batch
//     const userIds = Array.from(new Set(batch.map((event) => event.userId)));

//     // Check if user data is available in cache
//     const redisClient = RedisClient.getInstance();
//     const cachedUserData = await redisClient.MGET(userIds);

//     const usersToFetchFromDB: string[] = [];
//     const userUpdates = [];

//     // Process each like event
//     for (const event of batch) {
//       const cachedUser = null;
//       if (cachedUserData) cachedUserData[userIds.indexOf(event.userId)];
//       if (cachedUser) {
//         // Update user data from cache
//         const updatedUser = JSON.parse(cachedUser);
//         updatedUser.points += parseInt(event.points);
//         userUpdates.push(updatedUser);
//       } else {
//         // User data not in cache, fetch from DB
//         usersToFetchFromDB.push(event.userId);
//       }
//     }

//     // Fetch user data from DB for missing users
//     if (usersToFetchFromDB.length > 0) {
//       const usersFromDB = await UserModel.find({
//         _id: { $in: usersToFetchFromDB },
//       });
//       userUpdates.push(...usersFromDB);

//       // Update cache with fetched user data
//       for (const user of usersFromDB) {
//         await redisClient.Cache_Value(JSON.stringify(user._id), JSON.stringify(user));
//       }
//     }

//     console.log(`Processed ${batch.length} like events`);
//   } catch (error) {
//     console.error("Error processing batch:", error);
//   }
// }
async function processBatch(batch: LikeEvent[]) {
  try {
    const userIds = Array.from(new Set(batch.map((event) => event.userId)));

    const usersFromDB = await UserModel.find({ _id: { $in: userIds } });

    const userUpdates = [];

    for (const user of usersFromDB) {
      const eventsForUser = batch.filter(
        (event) => event.userId === user._id.toString()
      );
      if (eventsForUser.length > 0) {
        let points = 0;
        for (const event of eventsForUser) {
          points += parseInt(event.points);
        }
        user.points += points;
        userUpdates.push(user.save());
        // Update cache with fetched user data
      const cached=  await RedisClient.getInstance().Cache_Value(
          user._id.toString(),
          JSON.stringify(user)
        );
        console.log("cached:", cached);
      }
    }

    await Promise.all(userUpdates);

    console.log(`Processed ${batch.length} like events and pushed to db`);
  } catch (error) {
    console.error("Error processing batch:", error);
  }
}
