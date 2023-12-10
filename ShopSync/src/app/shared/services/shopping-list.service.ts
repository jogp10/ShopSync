import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, map, of } from 'rxjs';
import { AuthService } from './auth.service';

@Injectable({
  providedIn: 'root'
})
export class ShoppingListService {
  private apiUrl: string = 'http://127.0.0.1:5000/shopping_list/';

  constructor(private http: HttpClient, private authService: AuthService) {
    this.authService.user$.subscribe(user => {
      if (user) {
        const userPort = user.email.split('@')[0];
        this.apiUrl = `http://127.0.0.1:${userPort}/shopping_list/`;
      }
    });
  }

  getShoppingLists(): Observable<any> {
    return this.http.get<any>(this.apiUrl);
  }

  getShoppingList(id: string): Observable<any> {
    return this.http.get<any>(this.apiUrl + id);
  }

  getListNames(): Observable<any> {
    return this.getShoppingLists();
  }

  updateShoppingListItem(shoppingListUuid: string, itemName: string, itemQuantity: number): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
      item_name: itemName,
      item_quantity: itemQuantity
    };
  
    return this.http.post<any>(`${this.apiUrl}item/change`, data);
  }

  addShoppingListItem(shoppingListUuid: string, itemName: string, itemQuantity: number): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
      item_name: itemName,
      item_quantity: itemQuantity
    };
  
    return this.http.post<any>(`${this.apiUrl}item/add`, data);
  }

  deleteShoppingListItem(shoppingListUuid: string, itemName: string): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
      item_name: itemName,
    };
  
    return this.http.post<any>(`${this.apiUrl}item/delete`, data);
  }

  // TODO: Implement the following methods on frontend
  getShoppingListFromCloud(shoppingListUuid: string): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
    };
  
    return this.http.post<any>(`${this.apiUrl}load`, data);
  }

  getShoppingListsFromCloud(): Observable<any> {
    return this.http.post<any>(`${this.apiUrl}load_all`, {});
  }

  storeShoppingListToCloud(shoppingListUuid: string): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
    };
  
    return this.http.post<any>(`${this.apiUrl}sync`, data);
  }

  storeShoppingListsToCloud(): Observable<any> {
    return this.http.post<any>(`${this.apiUrl}sync_all`, {});
  }

  deleteShoppingList(shoppingListUuid: string): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
    };
  
    return this.http.post<any>(`${this.apiUrl}delete`, data);
  }

  createShoppingList(shoppingListName: string): Observable<any> {
    const data = {
      shopping_list_name: shoppingListName,
    };
  
    return this.http.post<any>(`${this.apiUrl}create`, data);
  }
}
