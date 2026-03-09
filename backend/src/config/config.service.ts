import { Injectable } from '@nestjs/common';
import * as dotenv from 'dotenv';

@Injectable()
export class ConfigService {
  private readonly config: Record<string, string>;

  constructor() {
    dotenv.config();
    this.config = {
      // DB2 Database
      DB_HOST: process.env.DB2_HOST || 'localhost',
      DB_PORT: process.env.DB2_PORT || '50000',
      DB_USERNAME: process.env.DB2_USER || 'db2inst1',
      DB_PASSWORD: process.env.DB2_PASSWORD || 'password',
      DB_NAME: process.env.DB2_DATABASE || 'bankdb',

      // PostgreSQL Database (target)
      PG_HOST: process.env.PG_HOST || 'localhost',
      PG_PORT: process.env.PG_PORT || '5432',
      PG_USER: process.env.PG_USER || 'postgres',
      PG_PASSWORD: process.env.PG_PASSWORD || 'postgres',
      PG_DATABASE: process.env.PG_DATABASE || 'bankdb',

      // RabbitMQ
      RABBITMQ_HOST: process.env.RABBITMQ_HOST || 'localhost',
      RABBITMQ_PORT: process.env.RABBITMQ_PORT || '5672',
      RABBITMQ_USER: process.env.RABBITMQ_USER || 'guest',
      RABBITMQ_PASSWORD: process.env.RABBITMQ_PASSWORD || 'guest',

      // App
      PORT: process.env.PORT || '3000',
      NODE_ENV: process.env.NODE_ENV || 'development',

      // Pipelines
      PIPELINES_DIR: process.env.PIPELINES_DIR || '../',
    };
  }

  get<T = string>(key: string): T {
    return this.config[key] as T;
  }

  getDatabaseConfig() {
    return {
      host: this.config.DB_HOST,
      port: parseInt(this.config.DB_PORT, 10),
      username: this.config.DB_USERNAME,
      password: this.config.DB_PASSWORD,
      database: this.config.DB_NAME,
    };
  }

  getPgConfig() {
    return {
      host: this.config.PG_HOST,
      port: parseInt(this.config.PG_PORT, 10),
      username: this.config.PG_USER,
      password: this.config.PG_PASSWORD,
      database: this.config.PG_DATABASE,
    };
  }

  getRabbitMQConfig() {
    return {
      host: this.config.RABBITMQ_HOST,
      port: parseInt(this.config.RABBITMQ_PORT, 10),
      username: this.config.RABBITMQ_USER,
      password: this.config.RABBITMQ_PASSWORD,
    };
  }
}