// src/services/mockHistoricalMetadataService.js

/**
 * Mock service to simulate historical metadata API based on connection_metadata table structure
 * This will be replaced with real API calls once backend is implemented
 */
class MockHistoricalMetadataService {
  constructor() {
    // Generate mock data based on the CSV structure
    this.mockData = this.generateMockData();
    this.connectionData = {}; // Cache for generated connection data
  }

  generateMockData() {
    // We'll generate data for any connection ID dynamically
    const tables = ['users', 'orders', 'products', 'payments', 'reviews'];
    const metadataTypes = ['tables', 'columns', 'statistics'];

    // Store the base data structure that can be used for any connection
    this.baseData = {
      tables,
      metadataTypes,
      startDate: new Date(),
      endDate: new Date()
    };

    // Set date range to last 30 days including today
    this.baseData.startDate.setDate(this.baseData.endDate.getDate() - 29);

    console.log('Mock service initialized with date range:',
      this.baseData.startDate.toISOString().split('T')[0],
      'to',
      this.baseData.endDate.toISOString().split('T')[0]
    );

    return []; // We'll generate data on-demand now
  }

  // Generate data on-demand for any connection ID
  generateDataForConnection(connectionId) {
    if (this.connectionData && this.connectionData[connectionId]) {
      return this.connectionData[connectionId];
    }

    if (!this.connectionData) {
      this.connectionData = {};
    }

    const data = [];
    const { tables, metadataTypes, startDate, endDate } = this.baseData;

    // Generate historical data for the date range
    for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
      for (const metadataType of metadataTypes) {
        // Generate metadata based on type
        let metadata = {};

        if (metadataType === 'tables') {
          metadata = this.generateTablesMetadata(tables, d);
        } else if (metadataType === 'columns') {
          metadata = this.generateColumnsMetadata(tables, d);
        } else if (metadataType === 'statistics') {
          metadata = this.generateStatisticsMetadata(tables, d);
        }

        data.push({
          id: `${connectionId}-${metadataType}-${d.toISOString().split('T')[0]}`,
          connection_id: connectionId,
          metadata_type: metadataType,
          metadata: JSON.stringify(metadata),
          collected_at: new Date(d),
          refresh_frequency: '1 day'
        });
      }
    }

    this.connectionData[connectionId] = data;
    return data;
  }

  generateTablesMetadata(tables, date) {
    const tablesData = {};

    tables.forEach((tableName, index) => {
      // Simulate table evolution over time
      const daysSinceStart = Math.floor((date - new Date('2024-01-01')) / (1000 * 60 * 60 * 24));

      // Some tables might be added later
      if (tableName === 'reviews' && daysSinceStart < 15) {
        return; // Reviews table added after 15 days
      }

      tablesData[tableName] = {
        name: tableName,
        schema: 'public',
        row_count: Math.floor(1000 + (daysSinceStart * 10) + (Math.random() * 100)),
        column_count: this.getColumnCountForTable(tableName, daysSinceStart),
        size_bytes: Math.floor(50000 + (daysSinceStart * 1000) + (Math.random() * 5000)),
        created_at: '2024-01-01T00:00:00Z',
        updated_at: date.toISOString()
      };
    });

    return { tables: tablesData };
  }

  generateColumnsMetadata(tables, date) {
    const columnsByTable = {};
    const daysSinceStart = Math.floor((date - new Date('2024-01-01')) / (1000 * 60 * 60 * 24));

    tables.forEach(tableName => {
      // Skip tables that don't exist yet
      if (tableName === 'reviews' && daysSinceStart < 15) {
        return;
      }

      columnsByTable[tableName] = this.getColumnsForTable(tableName, daysSinceStart);
    });

    return { columns_by_table: columnsByTable };
  }

  generateStatisticsMetadata(tables, date) {
    const statisticsByTable = {};
    const daysSinceStart = Math.floor((date - new Date('2024-01-01')) / (1000 * 60 * 60 * 24));

    tables.forEach(tableName => {
      if (tableName === 'reviews' && daysSinceStart < 15) {
        return;
      }

      const columns = this.getColumnsForTable(tableName, daysSinceStart);
      const columnStatistics = {};

      columns.forEach(column => {
        columnStatistics[column.name] = {
          type: column.data_type,
          basic: {
            null_count: Math.floor(Math.random() * 10),
            null_percentage: Math.random() * 5,
            distinct_count: Math.floor(Math.random() * 100),
            distinct_percentage: Math.random() * 50,
            is_unique: column.name.includes('id')
          },
          numeric: column.data_type.includes('int') || column.data_type.includes('decimal') ? {
            min: Math.floor(Math.random() * 100),
            max: Math.floor(Math.random() * 1000) + 100,
            avg: Math.floor(Math.random() * 500) + 50
          } : undefined,
          string: column.data_type.includes('varchar') || column.data_type.includes('text') ? {
            min_length: 1,
            max_length: Math.floor(Math.random() * 100) + 10,
            avg_length: Math.floor(Math.random() * 50) + 5
          } : undefined
        };
      });

      statisticsByTable[tableName] = {
        general: {
          row_count: Math.floor(1000 + (daysSinceStart * 10))
        },
        column_statistics: columnStatistics
      };
    });

    return { statistics_by_table: statisticsByTable };
  }

  getColumnsForTable(tableName, daysSinceStart) {
    const baseColumns = {
      users: [
        { name: 'id', data_type: 'bigint', nullable: false, ordinal_position: 1, column_default: null },
        { name: 'email', data_type: 'varchar(255)', nullable: false, ordinal_position: 2, column_default: null },
        { name: 'first_name', data_type: 'varchar(100)', nullable: true, ordinal_position: 3, column_default: null },
        { name: 'last_name', data_type: 'varchar(100)', nullable: true, ordinal_position: 4, column_default: null },
        { name: 'created_at', data_type: 'timestamp', nullable: false, ordinal_position: 5, column_default: 'CURRENT_TIMESTAMP' }
      ],
      orders: [
        { name: 'id', data_type: 'bigint', nullable: false, ordinal_position: 1, column_default: null },
        { name: 'user_id', data_type: 'bigint', nullable: false, ordinal_position: 2, column_default: null },
        { name: 'total_amount', data_type: 'decimal(10,2)', nullable: false, ordinal_position: 3, column_default: null },
        { name: 'status', data_type: 'varchar(50)', nullable: false, ordinal_position: 4, column_default: 'pending' },
        { name: 'created_at', data_type: 'timestamp', nullable: false, ordinal_position: 5, column_default: 'CURRENT_TIMESTAMP' }
      ],
      products: [
        { name: 'id', data_type: 'bigint', nullable: false, ordinal_position: 1, column_default: null },
        { name: 'name', data_type: 'varchar(255)', nullable: false, ordinal_position: 2, column_default: null },
        { name: 'description', data_type: 'text', nullable: true, ordinal_position: 3, column_default: null },
        { name: 'price', data_type: 'decimal(10,2)', nullable: false, ordinal_position: 4, column_default: null },
        { name: 'category_id', data_type: 'bigint', nullable: true, ordinal_position: 5, column_default: null }
      ],
      payments: [
        { name: 'id', data_type: 'bigint', nullable: false, ordinal_position: 1, column_default: null },
        { name: 'order_id', data_type: 'bigint', nullable: false, ordinal_position: 2, column_default: null },
        { name: 'amount', data_type: 'decimal(10,2)', nullable: false, ordinal_position: 3, column_default: null },
        { name: 'payment_method', data_type: 'varchar(50)', nullable: false, ordinal_position: 4, column_default: null }
      ],
      reviews: [
        { name: 'id', data_type: 'bigint', nullable: false, ordinal_position: 1, column_default: null },
        { name: 'product_id', data_type: 'bigint', nullable: false, ordinal_position: 2, column_default: null },
        { name: 'user_id', data_type: 'bigint', nullable: false, ordinal_position: 3, column_default: null },
        { name: 'rating', data_type: 'integer', nullable: false, ordinal_position: 4, column_default: null },
        { name: 'comment', data_type: 'text', nullable: true, ordinal_position: 5, column_default: null }
      ]
    };

    let columns = [...(baseColumns[tableName] || [])];

    // Simulate column evolution over time
    if (tableName === 'users') {
      // Add phone column after 10 days
      if (daysSinceStart >= 10) {
        columns.push({
          name: 'phone',
          data_type: 'varchar(20)',
          nullable: true,
          ordinal_position: 6,
          column_default: null
        });
      }

      // Change email type after 20 days (simulate a schema change)
      if (daysSinceStart >= 20) {
        const emailCol = columns.find(c => c.name === 'email');
        if (emailCol) {
          emailCol.data_type = 'varchar(320)'; // Updated to support longer emails
        }
      }
    }

    if (tableName === 'orders') {
      // Add shipping_address column after 15 days
      if (daysSinceStart >= 15) {
        columns.push({
          name: 'shipping_address',
          data_type: 'text',
          nullable: true,
          ordinal_position: 6,
          column_default: null
        });
      }
    }

    return columns;
  }

  getColumnCountForTable(tableName, daysSinceStart) {
    const baseCounts = {
      users: 5,
      orders: 5,
      products: 5,
      payments: 4,
      reviews: 5
    };

    let count = baseCounts[tableName] || 0;

    // Simulate column additions over time
    if (tableName === 'users' && daysSinceStart >= 10) count++;
    if (tableName === 'orders' && daysSinceStart >= 15) count++;

    return count;
  }

  // Public API methods

  async getAvailableDates(connectionId) {
    console.log('Getting available dates for connection:', connectionId);

    const data = this.generateDataForConnection(connectionId);

    const dates = data
      .filter(item => item.connection_id === connectionId)
      .map(item => item.collected_at.toISOString().split('T')[0])
      .filter((date, index, array) => array.indexOf(date) === index)
      .sort((a, b) => b.localeCompare(a)); // Most recent first

    console.log('Available dates found:', dates);
    return dates;
  }

  async getAvailableTables(connectionId) {
    console.log('Getting available tables for connection:', connectionId);

    const data = this.generateDataForConnection(connectionId);

    // Get the most recent tables metadata
    const recentTablesData = data
      .filter(item => item.connection_id === connectionId && item.metadata_type === 'tables')
      .sort((a, b) => b.collected_at - a.collected_at)[0];

    if (!recentTablesData) {
      console.log('No tables data found');
      return [];
    }

    const metadata = JSON.parse(recentTablesData.metadata);
    const tables = Object.keys(metadata.tables || {});

    console.log('Available tables found:', tables);
    return tables;
  }

  async getHistoricalMetadata(connectionId, date, metadataType, tableName = null) {
    console.log('Getting historical metadata:', { connectionId, date, metadataType, tableName });

    const data = this.generateDataForConnection(connectionId);

    const item = data.find(item =>
      item.connection_id === connectionId &&
      item.metadata_type === metadataType &&
      item.collected_at.toISOString().split('T')[0] === date
    );

    if (!item) {
      console.log('No metadata found for specified criteria');
      return null;
    }

    let metadata = JSON.parse(item.metadata);

    // Filter by table if specified
    if (tableName && metadataType === 'tables' && metadata.tables) {
      metadata = {
        tables: {
          [tableName]: metadata.tables[tableName]
        }
      };
    } else if (tableName && metadataType === 'columns' && metadata.columns_by_table) {
      metadata = {
        columns_by_table: {
          [tableName]: metadata.columns_by_table[tableName] || []
        }
      };
    } else if (tableName && metadataType === 'statistics' && metadata.statistics_by_table) {
      metadata = {
        statistics_by_table: {
          [tableName]: metadata.statistics_by_table[tableName]
        }
      };
    }

    const result = {
      id: item.id,
      connection_id: item.connection_id,
      metadata_type: item.metadata_type,
      metadata,
      collected_at: item.collected_at.toISOString(),
      refresh_frequency: item.refresh_frequency
    };

    console.log('Historical metadata found:', result);
    return result;
  }

  async getMetadataTimeline(connectionId, tableName = null, days = 30) {
    const data = this.generateDataForConnection(connectionId);

    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - days);

    const timeline = data
      .filter(item =>
        item.connection_id === connectionId &&
        item.collected_at >= startDate &&
        item.collected_at <= endDate
      )
      .map(item => ({
        date: item.collected_at.toISOString().split('T')[0],
        metadata_type: item.metadata_type,
        has_data: true
      }))
      .sort((a, b) => a.date.localeCompare(b.date));

    return timeline;
  }
}

export const mockHistoricalMetadataService = new MockHistoricalMetadataService();