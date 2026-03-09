import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule } from '../config/config.module';
import { ConfigService } from '../config/config.service';
import { DatabasesController } from './databases.controller';
import { DatabasesService } from './databases.service';

@Module({
  imports: [ConfigModule],
  controllers: [DatabasesController],
  providers: [DatabasesService],
  exports: [DatabasesService],
})
export class DatabasesModule {}