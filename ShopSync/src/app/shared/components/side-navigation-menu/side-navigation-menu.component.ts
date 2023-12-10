import { Component, NgModule, Output, Input, EventEmitter, ViewChild, ElementRef, AfterViewInit, OnDestroy } from '@angular/core';
import { DxTreeViewModule, DxTreeViewComponent, DxTreeViewTypes } from 'devextreme-angular/ui/tree-view';
import * as events from 'devextreme/events';
import { ShoppingListService } from '../../services/shopping-list.service';

@Component({
  selector: 'app-side-navigation-menu',
  templateUrl: './side-navigation-menu.component.html',
  styleUrls: ['./side-navigation-menu.component.scss']
})
export class SideNavigationMenuComponent implements AfterViewInit, OnDestroy {
  @ViewChild(DxTreeViewComponent, { static: true })
  menu!: DxTreeViewComponent;
  private menuItems: any[] = [];


  @Output()
  selectedItemChanged = new EventEmitter<DxTreeViewTypes.ItemClickEvent>();

  @Output()
  openMenu = new EventEmitter<any>();

  private _selectedItem!: String;
  @Input()
  set selectedItem(value: String) {
    this._selectedItem = value;
    if (!this.menu.instance) {
      return;
    }

    this.menu.instance.selectItem(value);
  }

  private _items!: Record <string, unknown>[];
  get items() {
    if (!this._items) {
      try {
        this.shoppingListService.getListNames().subscribe({
          next: (data: any) => {
            this._items = this.getItems(data);
          },
          error: (error: any) => {
            console.error('Error fetching dynamic navigation', error);
          },
        });
        // Additional processing if needed
      } catch (error) {
        console.error('Error fetching dynamic navigation', error);
      }
    }
  
    return this._items || [];
  }

  private _compactMode = false;
  @Input()
  get compactMode() {
    return this._compactMode;
  }
  set compactMode(val) {
    this._compactMode = val;

    if (!this.menu.instance) {
      return;
    }

    if (val) {
      this.menu.instance.collapseAll();
    } else {
      this.menu.instance.expandItem(this._selectedItem);
    }
  }

  constructor(private elementRef: ElementRef, private shoppingListService: ShoppingListService) { }

  onItemClick(event: DxTreeViewTypes.ItemClickEvent) {
    this.selectedItemChanged.emit(event);
  }

  private getItems(data: any): any[] {
    console.log('data', data);
    this.menuItems = data;
  
    // Add a new item for creating a list
    const createList = {
      text: 'Create New List',
      icon: 'plus',
      onClick: () => {
        // Handle the click event to open the modal for creating a new list
        // this.openCreateListModal();
        console.log('Create New List');
      },
    };
  
    return [
      {
        text: 'Home',
        path: '/home',
        icon: 'home',
      },
      {
        text: 'My Lists',
        icon: 'folder',
        items: [
          ...this.menuItems.map((menuItem: any) => ({
            text: menuItem.name,
            path: '/tasks/' + menuItem.id,
          })),
          createList, // Include the "Create New List" item
        ],
      },
    ];
  }

  ngAfterViewInit() {
    events.on(this.elementRef.nativeElement, 'dxclick', (e: Event) => {
      this.openMenu.next(e);
    });
  }

  ngOnDestroy() {
    events.off(this.elementRef.nativeElement, 'dxclick');
  }
}

@NgModule({
  imports: [ DxTreeViewModule ],
  declarations: [ SideNavigationMenuComponent ],
  exports: [ SideNavigationMenuComponent ]
})
export class SideNavigationMenuModule { }
