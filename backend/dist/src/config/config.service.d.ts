export declare class ConfigService {
    private readonly config;
    constructor();
    get<T = string>(key: string): T;
    getDatabaseConfig(): {
        host: string;
        port: number;
        username: string;
        password: string;
        database: string;
    };
    getPgConfig(): {
        host: string;
        port: number;
        username: string;
        password: string;
        database: string;
    };
    getRabbitMQConfig(): {
        host: string;
        port: number;
        username: string;
        password: string;
    };
}
