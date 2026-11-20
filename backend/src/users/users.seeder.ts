import { DataSource } from 'typeorm';
import { hash } from 'bcryptjs';
import { User } from './entities/user.entity';

const SUPERUSER = {
  email: 'shauritangaathanas@gmail.com',
  password: 'Mwalimu@2025',
  name: 'Athanas Shauritanga',
  role: 'superuser',
};

export async function seedUsers(dataSource: DataSource) {
  await dataSource.query(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      email VARCHAR(255) UNIQUE NOT NULL,
      password_hash VARCHAR(255) NOT NULL,
      name VARCHAR(255) NOT NULL,
      role VARCHAR(50) NOT NULL DEFAULT 'superuser',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  const repository = dataSource.getRepository(User);
  const existing = await repository.findOne({ where: { email: SUPERUSER.email } });

  if (!existing) {
    const passwordHash = await hash(SUPERUSER.password, 10);
    await repository.save(
      repository.create({
        email: SUPERUSER.email,
        passwordHash,
        name: SUPERUSER.name,
        role: SUPERUSER.role,
      }),
    );
    console.log(`Seeded superuser: ${SUPERUSER.email}`);
  }
}
