import { CrudInterface, Table } from '../types';

const low = require('lowdb');
const _ = require('lodash');
const clone = require('rfdc')();

export class LowDatabase implements CrudInterface {

    private internal;

    /**
     * Creates an instance of JsonDatabase, an implementation of CrudInterface for a json file.
     *
     * @param filename An optional filename for the database.
     */
    constructor(filename?: string) {
        let Adapter; 
        if(process.env.STORAGE === 'localstorage') 
            Adapter = require('lowdb/adapters/LocalStorage');
         else 
            Adapter = require('lowdb/adapters/FileSync');
        
        const adapter = new Adapter(filename || 'db');
        this.internal = low(adapter);
        this.init();
    }

    /**
     * Initiates the storage.
     */
    private init(): void {
        this.internal.defaults({ participant: [], stage: [], group: [], round: [], match: [], match_game: []  })
        .write();
    }

    /**
     * @param objValue
     * @param srcValue
     */
    private assignCustomizer(objValue: any, srcValue: any): any {
        return _.isObject(srcValue) ? _.assign(objValue, srcValue) : srcValue;
    }

    /**
     * Empties the database and `init()` it back.
     */
    public reset(): void {
        this.internal.setState({});
        this.init();
    }

    /**
     * Inserts a value in a table and returns its id.
     *
     * @param table Where to insert.
     * @param value What to insert.
     */
    public insert<T>(table: Table, value: T): Promise<number>;

    /**
     * Inserts multiple values in a table.
     *
     * @param table Where to insert.
     * @param values What to insert.
     */
    public insert<T>(table: Table, values: T[]): Promise<boolean>;

    /**
     * Inserts a unique value or multiple values in a table.
     *
     * @param table Name of the table.
     * @param arg A single value or an array of values.
     */
    public async insert<T>(table: Table, arg: T | T[]): Promise<number | boolean> {
        const collection = this.internal.get(table);
        let id = collection.size().value();

        if (!Array.isArray(arg)) {
            try {
                collection.push({ id, ...arg }).write();
            } catch (error) {
                return -1;
            }

            return id;
        }

        try {
            arg.map(object => ({ id: id++, ...object })).forEach(o => collection.push(o).write());
        } catch (error) {
            return false;
        }

        return true;
    }

    /**
     * Gets all data from a table. 
     *
     * @param table Where to get from.
     */
    public select<T>(table: Table): Promise<T[] | null>;

    /**
     * Gets specific data from a table.
     *
     * @param table Where to get from.
     * @param key What to get.
     */
    public select<T>(table: Table, key: number): Promise<T | null>;

    /**
     * Gets data from a table with a filter.
     *
     * @param table Where to get from.
     * @param filter An object to filter data.
     */
    public select<T>(table: Table, filter: Partial<T>): Promise<T[] | null>

    /**
     * Gets a unique elements, elements matching a filter or all the elements in a table.
     * 
     * @param table Name of the table.
     * @param arg An index or a filter.
     */
    public async select<T>(table: Table, arg?: number | Partial<T>): Promise<T | T[] | null> {
        const collection = this.internal.get(table);
        try {
            if (arg === undefined)
                return collection.value().map(clone);

            if (typeof arg === 'number')
                return clone(collection.find({id: arg}).value());

            const values = collection.filter(arg).value() || null;
            return values && values.map(clone);
        } catch (error) {
            return null;
        }
    }

    /**
     * Updates data in a table.
     *
     * @param table Where to update.
     * @param key What to update.
     * @param value How to update.
     */
    public update<T>(table: Table, key: number, value: T): Promise<boolean>;

    /**
     * Updates data in a table.
     *
     * @param table Where to update.
     * @param filter An object to filter data.
     * @param value How to update.
     */
    public update<T>(table: Table, filter: Partial<T>, value: Partial<T>): Promise<boolean>;

    /**
     * Updates one or multiple elements in a table.
     * 
     * @param table Name of the table.
     * @param arg An index or a filter.
     * @param value The whole object if arg is an index or the values to change if arg is a filter.
     */
    public async update<T>(table: Table, arg: number | Partial<T>, value: T | Partial<T>): Promise<boolean> {
        const collection = this.internal.get(table);
        if (typeof arg === 'number') {
            try {
                collection.find({id: arg}).assign(value, this.assignCustomizer).write();
                return true;
            } catch (error) {
                return false;
            }
        }

        const values = collection.filter(arg).value();
        if (!values) return false;

        values.forEach((v: any) => collection.find({id: v.id}).assignWith(value, this.assignCustomizer).write());
        return true;
    }

    /**
     * Delete data in a table, based on a filter.
     *
     * @param table Where to delete in.
     * @param filter An object to filter data or undefined to empty the table.
     */
    public async delete<T extends { [key: string]: unknown }>(table: Table, filter?: Partial<T>): Promise<boolean> {
        const collection = this.internal.get(table);

        if (!filter) {
            collection.remove().write();
            return true;
        }

        collection.remove(filter).write();
        return true;
    }
}